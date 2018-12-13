package IpInfo;

import java.io.Serializable;

public class cl_IpInfo implements Serializable{
	
	private String ip;
	private String hostname;
	private String city;
	private String region;
	private String country;
	private String org;
	private double latitude;
	private double longitude;
	
	public String getHostname() {
		return hostname;
	}
	public String getCity() {
		return city;
	}
	public String getRegion() {
		return region;
	}
	public String getCountry() {
		return country;
	}
	public String getOrg() {
		return org;
	}
	public double getLatitude() {
		return latitude;
	}
	public double getLongitude() {
		return longitude;
	}
		
	public String getIp() {
		return ip;
	}
	
	public void setIp(String ip) {
		this.ip = ip;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public void setOrg(String org) {
		this.org = org;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
		
}
