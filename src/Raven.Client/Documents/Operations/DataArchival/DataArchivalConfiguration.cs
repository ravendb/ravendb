using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.DataArchival
{
    /// <summary>
    /// The configuration for data archival in RavenDB.
    /// Allows setting archive frequency, maximum items to process, and enabling/disabling the feature.
    /// </summary>
    public class DataArchivalConfiguration : IDynamicJson
    {
        /// <summary>
        ///  Indicates whether data archival is disabled.
        /// </summary>
        public bool Disabled { get; set; }

        /// <summary>
        /// The frequency of archival operations in seconds.
        /// If null, the default frequency is used.
        /// </summary>
        public long? ArchiveFrequencyInSec { get; set; }

        /// <summary>
        /// The maximum number of items to process in each archival operation.
        /// If null, it defaults to <see cref="int.MaxValue"/>.
        /// </summary>
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
