using System;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public sealed class RevisionsBinConfiguration : IDynamicJson
    {
        /// <summary>
        /// Gets or sets a value indicating whether the revisions bin cleaner is disabled.
        /// </summary>
        /// <value>
        /// <c>true</c> if the cleaner is disabled; otherwise, <c>false</c>.
        /// </value>
        public bool Disabled { get; set; }

        /// <summary>
        /// Gets or sets the minimum age of revisions to keep in the database.
        /// The revisions-bin cleaner deletes revisions that are older than that.
        /// </summary>
        /// <value>
        /// The minimum <see cref="TimeSpan"/> that revisions must be kept before being eligible for deletion.
        /// A null value means no age restriction is applied.
        /// </value>
        public TimeSpan? MinimumEntriesAgeToKeep { get; set; }

        /// <summary>
        /// Gets or sets the frequency at which the revisions bin cleaner executes cleaning.
        /// </summary>
        /// <value>
        /// The <see cref="TimeSpan"/> defining how often the cleaner will check for and process old revisions.
        /// The default value is 5 minutes.
        /// </value>
        public TimeSpan RefreshFrequency { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets or sets the maximum number of items to process (read) in one cleaning cycle.
        /// </summary>
        /// <value>
        /// The maximum number of revisions to process during a single execution of the bin cleaner.
        /// The default value is 16,384.
        /// </value>
        public int MaxItemsToProcess { get; set; } = 16 * 1024;

        private bool Equals(RevisionsBinConfiguration other)
        {
            return Disabled == other.Disabled &&
                   MinimumEntriesAgeToKeep == other.MinimumEntriesAgeToKeep &&
                   RefreshFrequency == other.RefreshFrequency &&
                   MaxItemsToProcess == other.MaxItemsToProcess;
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
                hashCode = (hashCode * 397) ^ MaxItemsToProcess.GetHashCode();
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(MinimumEntriesAgeToKeep)] = MinimumEntriesAgeToKeep,
                [nameof(RefreshFrequency)] = RefreshFrequency,
                [nameof(MaxItemsToProcess)] = MaxItemsToProcess,
            };
        }

        public DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }
}
