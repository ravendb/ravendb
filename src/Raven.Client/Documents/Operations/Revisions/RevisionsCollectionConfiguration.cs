using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevisionsCollectionConfiguration
    {
        public long? MinimumRevisionsToKeep { get; set; }

        public TimeSpan? MinimumRevisionAgeToKeep { get; set; }

        public bool Disabled { get; set; }

        public bool PurgeOnDelete { get; set; }

        public int MaxRevisionsToDeleteUponDocumentUpdate { get; set; }

        protected bool Equals(RevisionsCollectionConfiguration other)
        {
            return MinimumRevisionsToKeep == other.MinimumRevisionsToKeep &&
                   MinimumRevisionAgeToKeep == other.MinimumRevisionAgeToKeep &&
                   Disabled == other.Disabled &&
                   PurgeOnDelete == other.PurgeOnDelete &&
                   MaxRevisionsToDeleteUponDocumentUpdate == other.MaxRevisionsToDeleteUponDocumentUpdate;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((RevisionsCollectionConfiguration)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = MinimumRevisionsToKeep?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ MinimumRevisionAgeToKeep?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ Disabled.GetHashCode();
                hashCode = (hashCode * 397) ^ PurgeOnDelete.GetHashCode();
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(MinimumRevisionsToKeep)] = MinimumRevisionsToKeep,
                [nameof(MinimumRevisionAgeToKeep)] = MinimumRevisionAgeToKeep,
                [nameof(PurgeOnDelete)] = PurgeOnDelete,
                [nameof(MaxRevisionsToDeleteUponDocumentUpdate)] = MaxRevisionsToDeleteUponDocumentUpdate
            };
        }
    }
}
