using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments
{
    /// <summary>
    /// Configuration for remote attachments functionality, including destinations, frequency, and upload settings.
    /// </summary>
    public sealed class RemoteAttachmentsConfiguration : IDynamicJson
    {
        internal static readonly StringComparer KeyComparer = StringComparer.OrdinalIgnoreCase;

        /// <summary>
        /// Gets or sets the dictionary of remote attachment destinations, keyed by destination name (case-insensitive).
        /// </summary>
        [JsonDeserializationStringDictionary(StringComparison.OrdinalIgnoreCase)]
        public Dictionary<string, RemoteAttachmentsDestinationConfiguration> Destinations { get; set; } = new(KeyComparer);

        /// <summary>
        /// Gets or sets the frequency (in seconds) at which the remote attachments process checks for new items to upload.
        /// </summary>
        public long? CheckFrequencyInSec { get; set; }
            
        /// <summary>
        /// Gets or sets the maximum number of items to process in a single batch.
        /// </summary>
        public long? MaxItemsToProcess { get; set; }

        /// <summary>
        /// Gets or sets the number of concurrent uploads allowed.
        /// </summary>
        public int? ConcurrentUploads { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether remote attachments functionality is disabled.
        /// </summary>
        public bool Disabled { get; set; }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            if (Destinations == null)
            {
                hashCode.Add(0);
            }
            else
            {
                hashCode.Add(Destinations.Count);

                var orderedKeys = Destinations.Keys.OrderBy(k => k, KeyComparer);
                foreach (var key in orderedKeys)
                {
                    // Use the KeyComparer to get the case-insensitive hash code for the key.
                    hashCode.Add(KeyComparer.GetHashCode(key));
                    hashCode.Add(Destinations[key] == null ? 0 : Destinations[key].GetHashCode());
                }
            }

            hashCode.Add(CheckFrequencyInSec);
            hashCode.Add(MaxItemsToProcess);
            hashCode.Add(ConcurrentUploads);
            hashCode.Add(Disabled);

            return hashCode.ToHashCode();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((RemoteAttachmentsConfiguration)obj);
        }

        private bool Equals(RemoteAttachmentsConfiguration other)
        {
            if (CheckFrequencyInSec != other.CheckFrequencyInSec)
                return false;
            if (MaxItemsToProcess != other.MaxItemsToProcess)
                return false;
            if (ConcurrentUploads != other.ConcurrentUploads)
                return false;
            if (Disabled != other.Disabled)
                return false;

            if (Destinations == null && other.Destinations == null)
                return true;

            if (Destinations == null || other.Destinations == null)
                return false;

            if (Destinations.Count != other.Destinations.Count)
                return false;

            foreach (var kvp in Destinations)
            {
                if (other.Destinations.TryGetValue(kvp.Key, out var otherConfig) == false)
                    return false;

                if (Equals(kvp.Value, otherConfig) == false)
                    return false;
            }

            return true;
        }

        internal bool HasDestination()
        {
            if (Disabled)
                return false;
            if (Destinations == null || Destinations.Count == 0)
                return false;

            return Destinations.Any(x => x.Value.HasUploader());
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Destinations)] = Destinations.ToJson(),
                [nameof(CheckFrequencyInSec)] = CheckFrequencyInSec,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess,
                [nameof(ConcurrentUploads)] = ConcurrentUploads,
                [nameof(Disabled)] = Disabled,
            };
        }

        internal void AssertConfiguration(string databaseName = null)
        {
            var databaseNameStr = string.IsNullOrEmpty(databaseName) ? string.Empty : $" for database '{databaseName}'";

            if (CheckFrequencyInSec <= 0)
                throw new InvalidOperationException($"Remote attachments check frequency{databaseNameStr} must be greater than 0.");
            if (MaxItemsToProcess <= 0)
                throw new InvalidOperationException($"Max items to process{databaseNameStr} must be greater than 0.");
            if (ConcurrentUploads <= 0)
                throw new InvalidOperationException($"Concurrent attachments uploads{databaseNameStr} must be greater than 0.");

            if (Destinations == null || Destinations.Count == 0)
            {
                // no destinations configured
                return;
            }

            var keys = new HashSet<string>(KeyComparer);
            foreach (var kvp in Destinations)
            {
                if (keys.Add(kvp.Key) == false)
                    throw new InvalidOperationException($"Destination key '{kvp.Key}' is duplicate. Duplicate keys are not allowed in remote attachments configuration{databaseNameStr}.");

                if (kvp.Value == null)
                    throw new InvalidOperationException($"Destination configuration for key {kvp.Key} is null{databaseNameStr}.");

                kvp.Value.AssertConfiguration(kvp.Key, databaseName);
            }
        }
    }
}
