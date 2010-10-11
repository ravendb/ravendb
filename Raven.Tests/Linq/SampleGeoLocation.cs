namespace Raven.Tests.Linq
{
    public static class SampleGeoLocation
    {
        public static string GeoHash(int lon, int lang)
        {
            return lon + "#" + lang;
        }
    }
}