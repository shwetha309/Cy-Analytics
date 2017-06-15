package getis;

import classes.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.*;
import java.util.*;

public class QueryState {
	public static void main(String args[]) {
/*		ProcessorContext context = new ProcessorContext();
		KeyValueStore<String, GridMap> hourlyStore = (KeyValueStore<String, GridMap>) context.getStateStore("GridMapStore");
		KeyValueIterator<String, GridMap> iter = hourlyStore.all();

		while(iter.hasNext()) {
			KeyValue<String, GridMap> entry = iter.next();
			if( entry.value != null) {	
				System.out.println(entry.key +" "+entry.value);
			}
		}*/
	}
}	
