package getis;

import classes.*;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.*;
import java.util.Objects;
import java.text.SimpleDateFormat;
import java.text.Format;
import java.util.Date;
import org.apache.kafka.streams.processor.*;
import java.util.*;

public class GScoreProcess extends AbstractProcessor<String, RegionSummary> {
	private ProcessorContext context;
	private KeyValueStore<String, Double> GScoreStore;
	public KeyValueStore <String, GridMap> GridMapStore2;
	public KeyValueStore <String, Long> Attacks_Store;
	long sum_attacks;
	
	public void init(ProcessorContext context) {
		this.context = context;
		this.context.schedule(100000);
		//this.regionStore = (KeyValueStore<String, RegionSummary>) context.getStateStore("RegionStore");
		this.GridMapStore2 = (KeyValueStore<String, GridMap>) context.getStateStore("GridMapStore");
		this.GScoreStore = (KeyValueStore<String, Double>) context.getStateStore("GScoreStore");
		this.Attacks_Store =  (KeyValueStore<String, Long>) context.getStateStore("AttackStore");
		sum_attacks = 0;
	}

	public void process(String key, RegionSummary value) {
		//System.out.println("Processing");
		double dlat = (value.latitude*100);
		double dlon = (value.longitude*100);
	
		int lat = (int) dlat;
		int lon = (int) dlon;

		if( (lat >= 4050 && lat <= 4090) && (lon >= -7425 && lon <= -7370))
		{
			//System.out.println("Processing");	
			sum_attacks += 1;
			String coords = lat+"##"+lon;
			if(this.Attacks_Store.get(coords) == null)
			{
				this.Attacks_Store.put(coords,1L);
			}
			else
			{
				this.Attacks_Store.put(coords, this.Attacks_Store.get(coords)+1L);
			}
		}

	}			
	
	public double calcGScore(ArrayList<String> neighbors) {

		if(sum_attacks == 0)
			System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
		double sum_wx=0.0, sum_w =0, w = 1.0, sum_ww = 0.0;
		double mean = (sum_attacks/(2276.0));
		double mean_sqx =  ( (sum_attacks * sum_attacks)/2276.0);
		double s =  (Math.sqrt ( mean_sqx - (mean * mean))) ;


		for(int i=0;i<neighbors.size();i++)
		{
			String cell = neighbors.get(i);
			long x = 0;
			if(this.Attacks_Store.get(cell) != null)	
			{
				x = this.Attacks_Store.get(cell);
			}
			sum_wx += w*x;
			sum_w += w;
			sum_ww += w*w;
		}
		
		double numerator = sum_wx -( mean*sum_w );
		double inter_denom = ((( 2276.0 * sum_ww) - ( sum_w * sum_w )) / 2275.0 );
		double denom = s * Math.sqrt( inter_denom );
		double g =  (numerator / denom ) ;
		//System.out.println("G"+g);
		return g;
			
	}

	public void punctuate(long timestamp) {
		
		System.out.println("PUNCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
		KeyValueIterator<String, GridMap> iter = this.GridMapStore2.all();
		
		while(iter.hasNext()) {
			KeyValue<String, GridMap> entry = iter.next();
			if( entry.value != null) {
				//entry.value.timestamp=Long.toString(timestamp);
				//context.forward(Long.toString(timestamp), entry.value);
				//System.out.println(entry.key+" "+entry.value);

				ArrayList<String> neighbors = entry.value.neighbors;
				double g = calcGScore(neighbors);
				//this.GScoreStore.put(entry.key,g);
				context.forward(entry.key,g+"");
				System.out.println(entry.key+" "+g);
			}
		}
		sum_attacks = 0;		
		this.GridMapStore2 = (KeyValueStore<String, GridMap>) context.getStateStore("GridMapStore");
		iter.close();
		context.commit();
	}

	public void close() {
	}
};
