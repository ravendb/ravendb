namespace Raven.Server.Rachis
{
    public class Negotiation
    {
        public long MidpointIndex { get; set; }
        public long MidpointTerm  { get; set; }
        public long MinIndex { get; set; }
        public long MaxIndex { get; set; }
    }
}