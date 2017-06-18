using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.Versioning
{
    public class VersioningConfigurationCollection
    {
        public long? MinimumRevisionsToKeep { get; set; }

        public TimeSpan? MinimumRevisionAgeToKeep { get; set; }

        public bool Active { get; set; }

        public bool PurgeOnDelete { get; set; }

        protected bool Equals(VersioningConfigurationCollection other)
        {
            return MinimumRevisionsToKeep == other.MinimumRevisionsToKeep && 
                MinimumRevisionAgeToKeep == other.MinimumRevisionAgeToKeep && 
                Active == other.Active && 
                PurgeOnDelete == other.PurgeOnDelete;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((VersioningConfigurationCollection)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = MinimumRevisionsToKeep?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ MinimumRevisionAgeToKeep?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ Active.GetHashCode();
                hashCode = (hashCode * 397) ^ PurgeOnDelete.GetHashCode();
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Active)] = Active,
                [nameof(MinimumRevisionsToKeep)] = MinimumRevisionsToKeep,
                [nameof(MinimumRevisionAgeToKeep)] = MinimumRevisionAgeToKeep,
                [nameof(PurgeOnDelete)] = PurgeOnDelete
            };
        }
    }
}