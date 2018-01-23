using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Expiration
{
    public class ExpirationConfiguration : IDynamicJson
    {
        public bool Disabled { get; set; }

        public long? DeleteFrequencyInSec { get; set; }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Disabled.GetHashCode() * 397) ^ DeleteFrequencyInSec.GetHashCode();
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
            return Disabled == other.Disabled && DeleteFrequencyInSec == other.DeleteFrequencyInSec;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(DeleteFrequencyInSec)] = DeleteFrequencyInSec
            };
        }
    }
}
