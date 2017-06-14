package classes;

public class AttackCategory
{
  	public double latitude;
	public double longitude;
	public String city;
	public Object country;
	
	public AttackCategory(double lat, double lon, String cty, Object ctry)
	{
		latitude = lat;
		longitude = lon;
		city = cty;
		country = ctry;
	}
}
