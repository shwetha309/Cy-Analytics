package AggregateCount;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.*;
import java.util.Objects;
import org.apache.kafka.streams.processor.*;


@SuppressWarnings("unchecked")
public class AggregateProcessor extends AbstractProcessor<String, Long> {
    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;
	System.out.println("Context Creation");
        // call this processor's punctuate() method every 1000 milliseconds.
        this.context.schedule(90000);

        // retrieve the key-value store named "Counts"
        this.kvStore = (KeyValueStore<String, Long>) context.getStateStore("Agg-Counts");
    }

    @Override
    public void process(String key, Long count) {
        
	System.out.println("Process");
        Long oldValue = this.kvStore.get(key);

        if (oldValue == null) {
            this.kvStore.put(key, count);
        } else {
            this.kvStore.put(key, oldValue + count);
        }
        
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Long> iter = this.kvStore.all();

        while (iter.hasNext()) {
            KeyValue<String, Long> entry = iter.next();
            context.forward(entry.key, entry.value);
        }

        iter.close();
        // commit the current processing progress
        context.commit();
    }

    @Override
    public void close() {
        // close any resources managed by this processor.
        // Note: Do not close any StateStores as these are managed
        // by the library
    }
}
