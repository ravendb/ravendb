using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.DataArchival
{
    public class DataArchivalConfiguration : IDynamicJson
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
            return Equals((DataArchivalConfiguration)obj);
        }

        protected bool Equals(DataArchivalConfiguration other)
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
