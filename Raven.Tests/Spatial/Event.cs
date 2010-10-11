namespace Raven.Tests.Spatial
{
    public class Event
    {
        public Event() { }

        public Event(string venue, double lat, double lng)
        {
            this.Venue = venue;
            this.Latitude = lat;
            this.Longitude = lng;
        }

        public string Venue { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }
}