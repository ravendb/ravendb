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
        /// A null value means no age restriction is applied.
        /// </value>
        public int? MinimumEntriesAgeToKeepInMin { get; set; }

        /// <summary>
        /// Gets or sets the frequency (in seconds) at which the revisions bin cleaner executes cleaning.
        /// </summary>
        /// <value>
        /// The <see cref="long"/> defining how often the cleaner will check for and process old entries (deleted docs with revisions).
        /// The default value is 5 minutes.
        /// </value>
        public long RefreshFrequencyInSec { get; set; } = 5 * 60;

        private bool Equals(RevisionsBinConfiguration other)
        {
            return Disabled == other.Disabled &&
                   MinimumEntriesAgeToKeepInMin == other.MinimumEntriesAgeToKeepInMin &&
                   RefreshFrequencyInSec == other.RefreshFrequencyInSec;
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
                hashCode = (hashCode * 397) ^ (MinimumEntriesAgeToKeepInMin?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ RefreshFrequencyInSec.GetHashCode();
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(MinimumEntriesAgeToKeepInMin)] = MinimumEntriesAgeToKeepInMin,
                [nameof(RefreshFrequencyInSec)] = RefreshFrequencyInSec
            };
        }

        public DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }
}
