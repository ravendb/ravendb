namespace Raven.Client.Server.expiration
{
    public class ExpirationConfiguration
    {
        public bool Active { get; set; }

        public long? DeleteFrequencySeconds { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ExpirationConfiguration)obj);
        }

        public bool Equals(ExpirationConfiguration other)
        {
            if (other == null)
                return false;
            return other.Active == Active &&
                   other.DeleteFrequencySeconds == DeleteFrequencySeconds;
        }
    }
}