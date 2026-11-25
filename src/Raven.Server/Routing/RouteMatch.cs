using Sparrow;

namespace Raven.Server.Routing
{
    public struct RouteMatch
    {
        public string Url;
        public string Method;
        public int CaptureStart;
        public int CaptureLength;
        public int MatchLength;

        public StringSegment GetCapture()
        {
            return new StringSegment(Url, CaptureStart, CaptureLength);
        }
    }
}
