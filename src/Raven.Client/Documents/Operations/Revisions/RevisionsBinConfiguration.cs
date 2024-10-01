using System;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public sealed class RevisionsBinConfiguration : IFillFromBlittableJson, IDynamicJson
    {
        public bool Disabled { get; set; }
        public TimeSpan? MinimumEntriesAgeToKeep { get; set; }
        public TimeSpan RefreshFrequency { get; set; } = TimeSpan.FromMinutes(5);
        public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromSeconds(15);
        public int MaxItemsToProcess { get; set; } = int.MaxValue;
        public int? NumberOfDeletesInBatch { get; set; }

        private bool Equals(RevisionsBinConfiguration other)
        {
            return Disabled == other.Disabled &&
                   MinimumEntriesAgeToKeep == other.MinimumEntriesAgeToKeep &&
                   RefreshFrequency == other.RefreshFrequency &&
                   CleanupInterval == other.CleanupInterval &&
                   MaxItemsToProcess == other.MaxItemsToProcess &&
                   NumberOfDeletesInBatch == other.NumberOfDeletesInBatch;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((RevisionsBinConfiguration)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Disabled.GetHashCode();
                hashCode = (hashCode * 397) ^ (MinimumEntriesAgeToKeep?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ RefreshFrequency.GetHashCode();
                hashCode = (hashCode * 397) ^ CleanupInterval.GetHashCode();
                hashCode = (hashCode * 397) ^ MaxItemsToProcess.GetHashCode();
                hashCode = (hashCode * 397) ^ (NumberOfDeletesInBatch?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            var configuration = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<RevisionsBinConfiguration>(json, "RevisionsConfiguration");
            Disabled = configuration.Disabled;
            MinimumEntriesAgeToKeep = configuration.MinimumEntriesAgeToKeep;
            RefreshFrequency = configuration.RefreshFrequency;
            CleanupInterval = configuration.CleanupInterval;
            MaxItemsToProcess = configuration.MaxItemsToProcess;
            NumberOfDeletesInBatch = configuration.NumberOfDeletesInBatch;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(MinimumEntriesAgeToKeep)] = MinimumEntriesAgeToKeep,
                [nameof(RefreshFrequency)] = RefreshFrequency,
                [nameof(CleanupInterval)] = CleanupInterval,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess,
                [nameof(NumberOfDeletesInBatch)] = NumberOfDeletesInBatch
            };
        }

        public DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }
}
