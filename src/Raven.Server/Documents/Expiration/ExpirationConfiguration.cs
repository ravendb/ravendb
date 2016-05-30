namespace Raven.Server.Documents.Expiration
{
    public class ExpirationConfiguration
    {
        public bool Active { get; set; }

        public long? DeleteFrequencySeconds { get; set; }
    }
}