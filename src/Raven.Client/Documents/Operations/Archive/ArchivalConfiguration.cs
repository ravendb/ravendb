using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Archival
{
    public class ArchivalConfiguration : IDynamicJson
    {
        public bool Disabled { get; set; }

        public long? ArchiveFrequencyInSec { get; set; }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Disabled.GetHashCode() * 397) ^ ArchiveFrequencyInSec.GetHashCode();
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ArchivalConfiguration)obj);
        }

        protected bool Equals(ArchivalConfiguration other)
        {
            return Disabled == other.Disabled && ArchiveFrequencyInSec == other.ArchiveFrequencyInSec;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(ArchiveFrequencyInSec)] = ArchiveFrequencyInSec
            };
        }
    }
}
