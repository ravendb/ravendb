namespace Raven.Client.Server.Expiration
{
    public class ExpirationConfiguration
    {
        public bool Active { get; set; }

        public long? DeleteFrequencySeconds { get; set; }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Active.GetHashCode() * 397) ^ DeleteFrequencySeconds.GetHashCode();
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ExpirationConfiguration)obj);
        }

        protected bool Equals(ExpirationConfiguration other)
        {
            return Active == other.Active && DeleteFrequencySeconds == other.DeleteFrequencySeconds;
        }
    }
}