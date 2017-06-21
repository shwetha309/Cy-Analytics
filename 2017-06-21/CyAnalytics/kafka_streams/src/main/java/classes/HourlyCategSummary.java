package classes;


public class HourlyCategSummary {
	public String timestamp;
	public int count;
	public String attack_type;

	public HourlyCategSummary(String type, int ct) {
		attack_type = type;
		count = ct;
	}
}
