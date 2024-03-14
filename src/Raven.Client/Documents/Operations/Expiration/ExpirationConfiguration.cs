using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Expiration
{
    public class ExpirationConfiguration : IDynamicJson
    {
        public bool Disabled { get; set; }

        public long? DeleteFrequencyInSec { get; set; }

        public long? MaxItemsToProcess { get; set; }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = Disabled.GetHashCode();
                hashCode = (hashCode * 397) ^ (DeleteFrequencyInSec?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (MaxItemsToProcess?.GetHashCode() ?? 0);
                return hashCode;
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
            return Disabled == other.Disabled && DeleteFrequencyInSec == other.DeleteFrequencyInSec && MaxItemsToProcess == other.MaxItemsToProcess;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(DeleteFrequencyInSec)] = DeleteFrequencyInSec,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess
            };
        }
    }
}
