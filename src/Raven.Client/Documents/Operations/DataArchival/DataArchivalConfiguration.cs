using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.DataArchival
{
    public class DataArchivalConfiguration : IDynamicJson
    {
        public bool Disabled { get; set; }

        public long? ArchiveFrequencyInSec { get; set; }

        public long? MaxItemsToProcess { get; set; }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = Disabled.GetHashCode();
                hashCode = (hashCode * 397) ^ (ArchiveFrequencyInSec?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (MaxItemsToProcess?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((DataArchivalConfiguration)obj);
        }

        protected bool Equals(DataArchivalConfiguration other)
        {
            return Disabled == other.Disabled && ArchiveFrequencyInSec == other.ArchiveFrequencyInSec && MaxItemsToProcess == other.MaxItemsToProcess;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(ArchiveFrequencyInSec)] = ArchiveFrequencyInSec,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess
            };
        }
    }
}
