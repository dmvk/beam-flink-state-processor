package org.apache.dmvk.beam;

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.junit.jupiter.api.Test;

public class BeamSavepointTest {

  private static class ExampleReadFunction implements BeamKeyedStateReaderFunction<String, Void> {

    private final StateTag<ValueState<String>> countsTag =
        StateTags.value("word_counts", StringUtf8Coder.of());

    @Override
    public Collection<? extends StateTag<?>> getTags() {
      return Collections.singletonList(countsTag);
    }

    @Override
    public void readKey(String key, StateNamespace stateNamespace, ReadContext<Void> context) {
      final ValueState<String> state = context.state(stateNamespace, countsTag);
      System.out.println(stateNamespace.stringKey() + " # " + key + " # " + state.read());
    }
  }

  @Test
  void testReadKeyedState() throws Exception {
    final ExecutionEnvironment executionEnvironment =
        ExecutionEnvironment.getExecutionEnvironment();
    final BeamSavepoint savepoint =
        BeamSavepoint.load(
            executionEnvironment, "data/savepoint-2d009b-a0b050cab584", new MemoryStateBackend());
    final DataSource<Void> voidDataSource =
        savepoint.readKeyedState(
            "Word count/ParMultiDo(Stateful)",
            new ExampleReadFunction(),
            StringUtf8Coder.of(),
            GlobalWindow.Coder.INSTANCE,
            Types.VOID);
    voidDataSource.print();
  }
}
