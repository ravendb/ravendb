using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Expiration
{
    /// <summary>
    /// The configuration for document expiration feature.
    /// Allows setting deletion frequency, maximum items to process, and enabling/disabling expiration.
    /// </summary>
    public sealed class ExpirationConfiguration : IDynamicJson
    {
        /// <summary>
        /// Indicates whether document expiration is disabled.
        /// </summary>
        public bool Disabled { get; set; }

        /// <summary>
        /// The frequency of document deletion operations in seconds.
        /// If null, the default frequency is used.
        /// </summary>
        public long? DeleteFrequencyInSec { get; set; }

        /// <summary>
        /// The maximum number of items to process in each expiration operation.
        /// If null, it defaults to <see cref="int.MaxValue"/>.
        /// </summary>
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

        private bool Equals(ExpirationConfiguration other)
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
