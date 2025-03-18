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
        /// Gets or sets the minimum age of revisions-bin entries (deleted docs with revisions) to keep in the database.
        /// The revisions-bin cleaner deletes the entries that are older than that.
        /// </summary>
        /// <value>
        /// The minimum <see cref="int"/> that revisions-bin entries (deleted docs with revisions) must be kept before being eligible for deletion.
        /// When set to 0, all revisions bin entries will be deleted.
        /// The default value is 30 days.
        /// </value>
        public int MinimumEntriesAgeToKeepInMin { get; set; } = 30 * 24 * 60;

        /// <summary>
        /// Gets or sets the frequency (in seconds) at which the revisions bin cleaner executes cleaning.
        /// </summary>
        /// <value>
        /// This parameter defines how often the cleaner will check for and process and delete old entries (deleted docs with revisions).
        /// The default value is 5 minutes.
        /// </value>
        public long CleanerFrequencyInSec { get; set; } = 5 * 60;

        private bool Equals(RevisionsBinConfiguration other)
        {
            return Disabled == other.Disabled &&
                   MinimumEntriesAgeToKeepInMin == other.MinimumEntriesAgeToKeepInMin &&
                   CleanerFrequencyInSec == other.CleanerFrequencyInSec;
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
                hashCode = (hashCode * 397) ^ MinimumEntriesAgeToKeepInMin.GetHashCode();
                hashCode = (hashCode * 397) ^ CleanerFrequencyInSec.GetHashCode();
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(MinimumEntriesAgeToKeepInMin)] = MinimumEntriesAgeToKeepInMin,
                [nameof(CleanerFrequencyInSec)] = CleanerFrequencyInSec
            };
        }

        public DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }
}
