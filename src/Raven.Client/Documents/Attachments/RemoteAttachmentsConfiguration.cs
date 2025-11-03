using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments
{
    public sealed class RemoteAttachmentsConfiguration : IDynamicJson
    {
        public Dictionary<string, RemoteAttachmentsDestinationConfiguration> Destinations { get; set; }
        public long? CheckFrequencyInSec { get; set; }
        public long? MaxItemsToProcess { get; set; }
        public int? ConcurrentUploads { get; set; }

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

            hashCode.Add(CheckFrequencyInSec);
            hashCode.Add(MaxItemsToProcess);
            hashCode.Add(ConcurrentUploads);

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
                [nameof(CheckFrequencyInSec)] = CheckFrequencyInSec,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess,
                [nameof(ConcurrentUploads)] = ConcurrentUploads,
            };
        }

        internal void AssertConfiguration(string databaseName = null)
        {
            var databaseNameStr = string.IsNullOrEmpty(databaseName) ? string.Empty : $" for database '{databaseName}'";

            if (HasUploader() == false)
                throw new InvalidOperationException($"Exactly one uploader for {nameof(RemoteAttachmentsConfiguration)}{databaseNameStr} must be configured.");

            foreach (var kvp in Destinations)
            {
                if (kvp.Value == null)
                    throw new InvalidOperationException($"Destination configuration for key {kvp.Key} is null{databaseNameStr}.");

                kvp.Value.AssertConfiguration(kvp.Key, databaseName);
            }

            if (CheckFrequencyInSec <= 0)
                throw new InvalidOperationException($"Remote attachments check frequency{databaseNameStr} must be greater than 0.");
            if (MaxItemsToProcess <= 0)
                throw new InvalidOperationException($"Max items to process{databaseNameStr} must be greater than 0.");
            if (ConcurrentUploads <= 0)
                throw new InvalidOperationException($"Concurrent attachments uploads{databaseNameStr} must be greater than 0.");

            if (Destinations.Any(x => BackupConfiguration.CanBackupUsing(x.Value.S3Settings)
                                        && BackupConfiguration.CanBackupUsing(x.Value.AzureSettings)))
                throw new InvalidOperationException($"Only one uploader for {nameof(RemoteAttachmentsConfiguration)}{databaseNameStr} can be configured.");
        }
    }
}
