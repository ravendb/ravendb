using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Attachments
{
    public sealed class RetireAttachmentsConfiguration : IDynamicJson
    {
        public bool Disabled { get; set; }
        public long? RetireFrequencyInSec { get; set; }
        public Dictionary<string, TimeSpan> RetirePeriods { get; set; }

        public long? MaxItemsToProcess { get; set; }

        public S3Settings S3Settings { get; set; }
        public AzureSettings AzureSettings { get; set; }
        // TODO: egor remove?
        public GlacierSettings GlacierSettings { get; set; }
        public GoogleCloudSettings GoogleCloudSettings { get; set; }
        public FtpSettings FtpSettings { get; set; }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();

            hashCode.Add(Disabled);
            hashCode.Add(RetireFrequencyInSec);

            foreach (var kvp in RetirePeriods)
            {
                hashCode.Add(kvp.Key, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(kvp.Value);
            }
            hashCode.Add(S3Settings);
            hashCode.Add(AzureSettings);
            hashCode.Add(GlacierSettings);
            hashCode.Add(GoogleCloudSettings);
            hashCode.Add(FtpSettings);
            return hashCode.ToHashCode();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((RetireAttachmentsConfiguration)obj);
        }

        private bool Equals(RetireAttachmentsConfiguration other)
        {
            if (Disabled != other.Disabled)
                return false;
            if (RetireFrequencyInSec != other.RetireFrequencyInSec)
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

            //if (GlacierSettings.Equals(other.GlacierSettings) == false)
            //    return false;
            //if (GoogleCloudSettings.Equals(other.GoogleCloudSettings) == false)
            //    return false;
            //if (FtpSettings.Equals(other.FtpSettings) == false)
            //    return false;

            var d1 = RetirePeriods;
            var d2 = other.RetirePeriods;

            bool dic = d1.Count == d2.Count && d1.All(
                (d1Kv) => d2.TryGetValue(d1Kv.Key, out var d2Value) && (
                    d1Kv.Value == d2Value ||
                    d1Kv.Value.Equals(d2Value)));

            return dic;


            //var x = B(S3Settings, other.S3Settings);
            //var y = B(AzureSettings, other.AzureSettings);

        }

        //private static bool B(BackupSettings b1, BackupSettings b2)
        //{
        //    if (b1 != null)
        //    {
        //        if (b2 == null)
        //            return false;
        //        if (b1.Equals(b2) == false)
        //            return false;
        //    }

        //    if (b1 == null && b2 != null)
        //    {
        //        return false;
        //    }

        //    return true;
        //}

        internal bool HasUploader() => BackupConfiguration.CanBackupUsing(S3Settings) ||
                                       BackupConfiguration.CanBackupUsing(AzureSettings);
        //&&
        //BackupConfiguration.CanBackupUsing(Configuration.GoogleCloudSettings) == false &&
        //BackupConfiguration.CanBackupUsing(Configuration.FtpSettings) == false &&
        //BackupConfiguration.CanBackupUsing(Configuration.GlacierSettings

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(RetireFrequencyInSec)] = RetireFrequencyInSec,
                [nameof(RetirePeriods)] = DynamicJsonValue.Convert(RetirePeriods),
                [nameof(S3Settings)] = S3Settings?.ToJson(),
                [nameof(AzureSettings)] = AzureSettings?.ToJson(),
                [nameof(GlacierSettings)] = GlacierSettings?.ToJson(),
                [nameof(GoogleCloudSettings)] = GoogleCloudSettings?.ToJson(),
                [nameof(FtpSettings)] = FtpSettings?.ToJson()
            };
        }
    }
}
