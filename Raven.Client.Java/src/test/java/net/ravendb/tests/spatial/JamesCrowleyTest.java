package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractMultiMapIndexCreationTask;

import org.junit.Test;



public class JamesCrowleyTest extends RemoteClientTest {

  @Test
  public void geoSpatialTest() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new EventsBySimpleLocation().execute(store);

      try (IDocumentSession session = store.openSession()) {
        EventVenue venue = new EventVenue();
        venue.setName("TechHub");
        venue.setAddressLine1("Sofia House");
        venue.setCity("London");
        venue.setPostalCode("EC1Y 2BJ");
        venue.setGeoLocation(new GeoLocation(38.9690000, -77.3862000));
        session.store(venue);

        EventListing eventListing = new EventListing("Some event");
        eventListing.setCost("free");
        eventListing.setEventType(EventType.CONFERENCE);
        eventListing.setVenueId(venue.getId());

        session.store(eventListing);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        List<EventWithLocation> matchingEvents = session.advanced().luceneQuery(EventWithLocation.class, EventsBySimpleLocation.class)
          .waitForNonStaleResultsAsOfNow(5 * 60 * 1000)
          .toList();
        assertEquals(1, matchingEvents.size());

      }
    }
  }

  public static enum EventType {
    CONFERENCE;
  }

  public static class EventVenue {
    private String name;
    private String addressLine1;
    private String postalCode;
    private String city;
    private GeoLocation geoLocation;
    private String id;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getAddressLine1() {
      return addressLine1;
    }

    public void setAddressLine1(String addressLine1) {
      this.addressLine1 = addressLine1;
    }

    public String getPostalCode() {
      return postalCode;
    }

    public void setPostalCode(String postalCode) {
      this.postalCode = postalCode;
    }

    public String getCity() {
      return city;
    }

    public void setCity(String city) {
      this.city = city;
    }

    public GeoLocation getGeoLocation() {
      return geoLocation;
    }

    public void setGeoLocation(GeoLocation geoLocation) {
      this.geoLocation = geoLocation;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

  }

  public static class GeoLocation {
    private double latitude;
    private double longitude;

    public GeoLocation(double lat, double lng) {
      this.latitude = lat;
      this.longitude = lng;
    }

    public GeoLocation() {
      //empty by design
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
  }

  public static class EventWithLocation {
    private String eventName;
    private String veneuName;
    private String venueId;
    private double lat;
    private double lng;
    private String eventId;

    public String getEventName() {
      return eventName;
    }

    public void setEventName(String eventName) {
      this.eventName = eventName;
    }

    public String getVeneuName() {
      return veneuName;
    }

    public void setVeneuName(String veneuName) {
      this.veneuName = veneuName;
    }

    public String getVenueId() {
      return venueId;
    }

    public void setVenueId(String venueId) {
      this.venueId = venueId;
    }

    public double getLat() {
      return lat;
    }

    public void setLat(double lat) {
      this.lat = lat;
    }

    public double getLng() {
      return lng;
    }

    public void setLng(double lng) {
      this.lng = lng;
    }

    public String getEventId() {
      return eventId;
    }

    public void setEventId(String eventId) {
      this.eventId = eventId;
    }

  }

  public static class EventListing {
    private String name;

    public EventListing(String name) {
      this.name = name;
    }

    private String cost;
    private EventType eventType;
    private String venueId;
    private String id;


    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getCost() {
      return cost;
    }

    public void setCost(String cost) {
      this.cost = cost;
    }

    public EventType getEventType() {
      return eventType;
    }

    public void setEventType(EventType eventType) {
      this.eventType = eventType;
    }

    public String getVenueId() {
      return venueId;
    }

    public void setVenueId(String venueId) {
      this.venueId = venueId;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }
  }

  public static class EventsBySimpleLocation extends AbstractMultiMapIndexCreationTask {
    public EventsBySimpleLocation() {
      addMap("from e in docs.EventWithLocations "
        + "select new { "
        + "    VenueId = e.VenueId, "
        + "    EventId = e.__document_id, "
        + "    EventName = e.Name, "
        + "    VenueName = (string)null, "
        + "    Long = 0, "
        + "    Lat = 0, "
        + "    _ = (object)null, "
        + "}");
      addMap("from v in docs.EventVenues "
        + "select new  { "
        + "    VenueId = v.__document_id, "
        + "    EventId = (string)null, "
        + "    EventName = (string)null, "
        + "    VenueName = v.Name, "
        + "    Long = v.GeoLocation.Longitude, "
        + "    Lat = v.GeoLocation.Latitude, "
        + "    _ = (object) null,  "
        + "}");
      reduce =

        " results.GroupBy(result => result.VenueId).Select(g => new { " +
          "     g = g, " +
          "     latitude = DynamicEnumerable.FirstOrDefault(g.Select(x => x.Lat), t => t != 0) ?? " +
          " default(double) " +
          " }).Select(this2 => new { " +
          "     this2 = this2, " +
          "     longitude = DynamicEnumerable.FirstOrDefault(this2.g.Select(x => x.Long), t => t != 0) ?? " +
          " default(double) " +
          " }).Select(this3 => new { " +
          "     VenueId = this3.this2.g.Key, " +
          "     EventId = DynamicEnumerable.FirstOrDefault(this3.this2.g.Select(x => x.EventId), x => x != (object) null), " +
          "     VenueName = DynamicEnumerable.FirstOrDefault(this3.this2.g.Select(x => x.VenueName), x => x != (object) null), " +
          "     EventName = DynamicEnumerable.FirstOrDefault(this3.this2.g.Select(x => x.EventName), x => x != (object) null), " +
          "     Lat = this3.this2.latitude, " +
          "     Long = this3.longitude, " +
          "     _ = AbstractIndexCreationTask.SpatialGenerate(((double ? ) this3.this2.latitude), ((double ? ) this3.longitude)) " +
          " })";

    }
  }


}
