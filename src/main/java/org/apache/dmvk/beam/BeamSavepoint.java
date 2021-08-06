package org.apache.dmvk.beam;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.FlinkKeyUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.util.NoOpFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.KeyedStateInputFormat;
import org.apache.flink.state.api.input.operator.StateReaderOperator;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

/** A simple wrapper around Flink State Processor API, that is compatible with Beam State API. */
public class BeamSavepoint {

  public static BeamSavepoint load(
      ExecutionEnvironment environment, String path, StateBackend stateBackend) throws IOException {
    CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);
    int maxParallelism =
        metadata.getOperatorStates().stream()
            .map(OperatorState::getMaxParallelism)
            .max(Comparator.naturalOrder())
            .orElseThrow(
                () -> new RuntimeException("Savepoint must contain at least one operator state."));
    SavepointMetadata savepointMetadata =
        new SavepointMetadata(
            maxParallelism, metadata.getMasterStates(), metadata.getOperatorStates());
    return new BeamSavepoint(environment, savepointMetadata, stateBackend);
  }

  private static class Context<OutputT>
      implements BeamKeyedStateReaderFunction.ReadContext<OutputT> {

    private final StateInternals stateInternals;
    private final Collector<OutputT> collector;

    private Context(StateInternals stateInternals, Collector<OutputT> collector) {
      this.stateInternals = stateInternals;
      this.collector = collector;
    }

    @Override
    public <T extends State> T state(StateNamespace namespace, StateTag<T> address) {
      return stateInternals.state(namespace, address);
    }

    @Override
    public void collect(OutputT record) {
      collector.collect(record);
    }

    @Override
    public void close() {
      collector.close();
    }
  }

  private static class BeamStateReaderOperator<KeyT, WindowT extends BoundedWindow, OutputT>
      extends StateReaderOperator<NoOpFunction, ByteBuffer, String, OutputT> {

    private final SerializablePipelineOptions pipelineOptions =
        new SerializablePipelineOptions(PipelineOptionsFactory.create());

    private final Coder<KeyT> keyCoder;
    private final Coder<WindowT> windowCoder;
    private final BeamKeyedStateReaderFunction<KeyT, OutputT> readFunction;

    @Nullable private transient FlinkStateInternals<KeyT> stateInternals;

    public BeamStateReaderOperator(
        BeamKeyedStateReaderFunction<KeyT, OutputT> readFunction,
        Coder<KeyT> keyCoder,
        Coder<WindowT> windowCoder,
        TypeSerializer<String> namespaceSerializer) {
      super(new NoOpFunction(), TypeInformation.of(ByteBuffer.class), namespaceSerializer);
      this.keyCoder = keyCoder;
      this.windowCoder = windowCoder;
      this.readFunction = readFunction;
    }

    @Override
    public void open() throws Exception {
      super.open();
      stateInternals = new FlinkStateInternals<>(getKeyedStateBackend(), keyCoder, pipelineOptions);
    }

    @Override
    public void processElement(
        ByteBuffer encodedKey, String namespace, Collector<OutputT> collector) throws Exception {
      final StateNamespace stateNamespace = StateNamespaces.fromString(namespace, windowCoder);
      final KeyT key = FlinkKeyUtils.decodeKey(encodedKey, keyCoder);
      readFunction.readKey(key, stateNamespace, new Context<>(stateInternals, collector));
    }

    @Override
    public CloseableIterator<Tuple2<ByteBuffer, String>> getKeysAndNamespaces(
        SavepointRuntimeContext ctx) {
      final Collection<? extends StateTag<?>> tags = readFunction.getTags();
      // TODO support multiple tags ...
      final StateTag<?> onlyTag = Iterables.getOnlyElement(tags);
      return new IteratorWithRemove<>(getKeyedStateBackend().getKeysAndNamespaces(onlyTag.getId()));
    }
  }

  private static class IteratorWithRemove<T> implements CloseableIterator<T> {

    private final Iterator<T> iterator;

    private final AutoCloseable resource;

    private IteratorWithRemove(Stream<T> stream) {
      this.iterator = stream.iterator();
      this.resource = stream;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      return iterator.next();
    }

    @Override
    public void remove() {}

    @Override
    public void close() throws Exception {
      resource.close();
    }
  }

  private final ExecutionEnvironment environment;
  private final SavepointMetadata metadata;
  private final StateBackend stateBackend;

  private BeamSavepoint(
      ExecutionEnvironment environment, SavepointMetadata metadata, StateBackend stateBackend) {
    this.environment = environment;
    this.metadata = metadata;
    this.stateBackend = stateBackend;
  }

  public <KeyT, WindowT extends BoundedWindow, OutputT> DataSource<OutputT> readKeyedState(
      String uid,
      BeamKeyedStateReaderFunction<KeyT, OutputT> readerFunction,
      Coder<KeyT> keyCoder,
      Coder<WindowT> windowCoder,
      TypeInformation<OutputT> outputType)
      throws IOException {
    final OperatorState operatorState = metadata.getOperatorState(uid);
    final KeyedStateInputFormat<ByteBuffer, String, OutputT> inputFormat =
        new KeyedStateInputFormat<>(
            operatorState,
            stateBackend,
            environment.getConfiguration(),
            new BeamStateReaderOperator<>(
                readerFunction,
                keyCoder,
                windowCoder,
                Types.STRING.createSerializer(environment.getConfig())));
    return environment.createInput(inputFormat, outputType);
  }
}
