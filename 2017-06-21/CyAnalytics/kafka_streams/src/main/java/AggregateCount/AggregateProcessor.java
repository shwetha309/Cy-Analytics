package AggregateCount;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.*;
import java.util.Objects;
import org.apache.kafka.streams.processor.*;
import classes.*;

@SuppressWarnings("unchecked")
public class AggregateProcessor extends AbstractProcessor<String, HourlyCategSummary> {
    private ProcessorContext context;
    private KeyValueStore<String, HourlyCategSummary> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;
	System.out.println("Context Creation");
        // call this processor's punctuate() method every 1000 milliseconds.
        this.context.schedule(3600);

        // retrieve the key-value store named "Counts"
        this.kvStore = (KeyValueStore<String, HourlyCategSummary>) context.getStateStore("AggCounts");
    }

    @Override
    public void process(String key, HourlyCategSummary value) {
        
	System.out.println("processing");	
        HourlyCategSummary oldValue = this.kvStore.get(key);

        if (oldValue == null) {
            this.kvStore.put(key, value);
        } else {
            this.kvStore.put(key, new HourlyCategSummary(key, (oldValue.count + value.count)));
        }
        
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, HourlyCategSummary> iter = this.kvStore.all();

        while (iter.hasNext()) {
            KeyValue<String, HourlyCategSummary> entry = iter.next();
            context.forward(entry.key, entry.value);
	    System.out.println(String.valueOf(entry.value));
        }
	
        this.kvStore = (KeyValueStore<String, HourlyCategSummary>) context.getStateStore("AggCounts");
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
