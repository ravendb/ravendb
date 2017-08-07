namespace Raven.Client.ServerWide.Expiration
{
    public class ExpirationConfiguration
    {
        public bool Active { get; set; }

        public long? DeleteFrequencyInSec { get; set; }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Active.GetHashCode() * 397) ^ DeleteFrequencyInSec.GetHashCode();
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
            return Active == other.Active && DeleteFrequencyInSec == other.DeleteFrequencyInSec;
        }
    }
}