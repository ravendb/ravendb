using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments
{
    public sealed class RetiredAttachmentsConfiguration : IDynamicJson
    {
        public Dictionary<string, RetiredAttachmentsDestinationConfiguration> Destinations { get; set; }
        public long? RetireFrequencyInSec { get; set; }
        public long? MaxItemsToProcess { get; set; }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            if (Destinations == null)
            {
                hashCode.Add(0);
            }
            else
            {
                foreach (var kvp in Destinations)
                {
                    hashCode.Add(kvp.Key.GetHashCode());
                    hashCode.Add(kvp.Value.GetHashCode());
                }
            }

            hashCode.Add(RetireFrequencyInSec);
            hashCode.Add(MaxItemsToProcess);

            return hashCode.ToHashCode();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((RetiredAttachmentsConfiguration)obj);
        }

        private bool Equals(RetiredAttachmentsConfiguration other)
        {
            if (RetireFrequencyInSec != other.RetireFrequencyInSec)
                return false;
            if (MaxItemsToProcess != other.MaxItemsToProcess)
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
                if (kvp.Value.Equals(otherConfig) == false)
                    return false;
            }
            return true;
        }

        internal bool HasUploader()
        {
            if (Destinations == null || Destinations.Count == 0)
                return false;

            return Destinations.Any(x => BackupConfiguration.CanBackupUsing(x.Value.S3Settings)
                                         || BackupConfiguration.CanBackupUsing(x.Value.AzureSettings));
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Destinations)] = Destinations.ToJson(),
                [nameof(RetireFrequencyInSec)] = RetireFrequencyInSec,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess,
            };
        }

        internal void AssertConfiguration(string databaseName = null)
        {
            var databaseNameStr = string.IsNullOrEmpty(databaseName) ? string.Empty : $" for database '{databaseName}'";

            if (HasUploader() == false)
                throw new InvalidOperationException($"Exactly one uploader for {nameof(RetiredAttachmentsConfiguration)}{databaseNameStr} must be configured.");

            foreach (var kvp in Destinations)
            {
                if (kvp.Value == null)
                    throw new InvalidOperationException($"Destination configuration for key {kvp.Key} is null{databaseNameStr}.");

                kvp.Value.AssertConfiguration(kvp.Key, databaseName);
            }

            if (RetireFrequencyInSec <= 0)
                throw new InvalidOperationException($"Retire attachments frequency{databaseNameStr} must be greater than 0.");
            if (MaxItemsToProcess <= 0)
                throw new InvalidOperationException($"Max items to process{databaseNameStr} must be greater than 0.");

            if (Destinations.Any(x => BackupConfiguration.CanBackupUsing(x.Value.S3Settings)
                                        && BackupConfiguration.CanBackupUsing(x.Value.AzureSettings)))
                throw new InvalidOperationException($"Only one uploader for {nameof(RetiredAttachmentsConfiguration)}{databaseNameStr} can be configured.");
        }
    }
}
