namespace Raven.Server.Documents.Indexes.Spatial
{
    public class Coordinates
    {
        public double Latitude;
        public double Longitude;

        private Coordinates()
        {
        }

        public Coordinates(double latitude, double longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
        }
    }
}
