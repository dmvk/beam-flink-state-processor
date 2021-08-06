package org.apache.dmvk.beam;

import java.io.Serializable;
import java.util.Collection;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.state.State;
import org.apache.flink.util.Collector;

public interface BeamKeyedStateReaderFunction<KeyT, OutputT> extends Serializable {

  interface ReadContext<OutputT> extends Collector<OutputT> {

    <T extends State> T state(StateNamespace namespace, StateTag<T> address);
  }

  Collection<? extends StateTag<?>> getTags();

  void readKey(KeyT key, StateNamespace stateNamespace, ReadContext<OutputT> context)
      throws Exception;
}
