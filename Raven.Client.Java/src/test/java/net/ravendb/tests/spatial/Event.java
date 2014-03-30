package net.ravendb.tests.spatial;

import java.util.Date;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Event {

  private String venue;
  private double latitude;
  private double longitude;
  private Date date;
  private int capacity;


  public Event() {
    super();
  }

  public Event(String venue, double latitude, double longitude) {
    super();
    this.venue = venue;
    this.latitude = latitude;
    this.longitude = longitude;
  }
  public Event(String venue, double latitude, double longitude, Date date) {
    super();
    this.venue = venue;
    this.latitude = latitude;
    this.longitude = longitude;
    this.date = date;
  }
  public Event(String venue, double latitude, double longitude, Date date, int capacity) {
    super();
    this.venue = venue;
    this.latitude = latitude;
    this.longitude = longitude;
    this.date = date;
    this.capacity = capacity;
  }
  public String getVenue() {
    return venue;
  }
  public void setVenue(String venue) {
    this.venue = venue;
  }
  public double getLatitude() {
    return latitude;
  }
  public void setLatitude(double latitude) {
    this.latitude = latitude;
  }
  public double getLongitude() {
    return longitude;
  }
  public void setLongitude(double longitude) {
    this.longitude = longitude;
  }
  public Date getDate() {
    return date;
  }
  public void setDate(Date date) {
    this.date = date;
  }
  public int getCapacity() {
    return capacity;
  }
  public void setCapacity(int capacity) {
    this.capacity = capacity;
  }



}
