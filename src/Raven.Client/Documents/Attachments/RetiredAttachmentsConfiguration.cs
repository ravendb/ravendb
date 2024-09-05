using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments
{
    public sealed class RetiredAttachmentsConfiguration : IDynamicJson
    {
        public bool Disabled { get; set; }
        public S3Settings S3Settings { get; set; }
        public AzureSettings AzureSettings { get; set; }

        public Dictionary<string, TimeSpan> RetirePeriods { get; set; }
        public long? RetireFrequencyInSec { get; set; }
        public long? MaxItemsToProcess { get; set; }

        // TODO: egor we need to make those setting per collection
        /// <summary>
        /// Purge the retired attachments when the document is deleted.
        /// Default: false
        /// </summary>
        public bool PurgeOnDelete { get; set; }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();

            hashCode.Add(Disabled);
            hashCode.Add(S3Settings);
            hashCode.Add(AzureSettings);

            foreach (var kvp in RetirePeriods)
            {
                hashCode.Add(kvp.Key, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(kvp.Value);
            }
  
            hashCode.Add(RetireFrequencyInSec);
            hashCode.Add(MaxItemsToProcess);
            hashCode.Add(PurgeOnDelete);

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
            if (Disabled != other.Disabled)
                return false;
            if (RetireFrequencyInSec != other.RetireFrequencyInSec)
                return false;
            if (MaxItemsToProcess != other.MaxItemsToProcess)
                return false;
            if (PurgeOnDelete != other.PurgeOnDelete)
                return false;

            if (S3Settings != null)
            {
                if (other.S3Settings == null)
                    return false;
                if (S3Settings.Equals(other.S3Settings) == false)
                    return false;
            }
            if (S3Settings == null && other.S3Settings != null)
            {
                return false;
            }

            if (AzureSettings != null)
            {
                if (other.AzureSettings == null)
                    return false;
                if (AzureSettings.Equals(other.AzureSettings) == false)
                    return false;
            }
            if (AzureSettings == null && other.AzureSettings != null)
            {
                return false;
            }

            var d1 = RetirePeriods;
            var d2 = other.RetirePeriods;

            bool dic = d1.Count == d2.Count && d1.All(
                (d1Kv) => d2.TryGetValue(d1Kv.Key, out var d2Value) && (
                    d1Kv.Value == d2Value ||
                    d1Kv.Value.Equals(d2Value)));

            return dic;
        }

        internal bool HasUploader() => BackupConfiguration.CanBackupUsing(S3Settings) ||
                                       BackupConfiguration.CanBackupUsing(AzureSettings);

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(RetireFrequencyInSec)] = RetireFrequencyInSec,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess,
                [nameof(RetirePeriods)] = DynamicJsonValue.Convert(RetirePeriods),
                [nameof(S3Settings)] = S3Settings?.ToJson(),
                [nameof(AzureSettings)] = AzureSettings?.ToJson(),
                [nameof(PurgeOnDelete)] = PurgeOnDelete
            };
        }

        internal void AssertConfiguration(string databaseName = null)
        {
            var databaseNameStr = string.IsNullOrEmpty(databaseName) ? string.Empty : $" for database '{databaseName}'";

            if (Disabled == false)
            {
                if (RetireFrequencyInSec == null)
                    throw new InvalidOperationException($"{nameof(RetireFrequencyInSec)}{databaseNameStr} must have a value when {nameof(Disabled)} is false.");

                if (RetireFrequencyInSec <= 0)
                    throw new InvalidOperationException($"Retire attachments frequency{databaseNameStr} must be greater than 0.");
                if(MaxItemsToProcess <= 0)
                    throw new InvalidOperationException($"Max items to process{databaseNameStr} must be greater than 0.");

                if (RetirePeriods == null || RetirePeriods.Count == 0)
                    throw new InvalidOperationException($"{nameof(RetirePeriods)}{databaseNameStr} must have a value when {nameof(Disabled)} is false.");

                if (RetirePeriods.Keys.Any(string.IsNullOrWhiteSpace))
                    throw new InvalidOperationException($"{nameof(RetirePeriods)}{databaseNameStr}  must have non empty keys.");

                if (RetirePeriods.Values.Any(x => x.TotalSeconds <= 0))
                    throw new InvalidOperationException($"{nameof(RetirePeriods)}{databaseNameStr} must have positive TimeSpan values.");

                if (HasUploader() == false)
                    throw new InvalidOperationException($"Exactly one uploader for {nameof(RetiredAttachmentsConfiguration)}{databaseNameStr} must be configured when {nameof(Disabled)} is false.");

                if (BackupConfiguration.CanBackupUsing(S3Settings) && BackupConfiguration.CanBackupUsing(AzureSettings))
                    throw new InvalidOperationException($"Only one uploader for {nameof(RetiredAttachmentsConfiguration)}{databaseNameStr} can be configured when {nameof(Disabled)} is false.");
            }
        }
    }
}
