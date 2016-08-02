using System.Collections.Generic;
using Raven.Abstractions.Json.Linq;

namespace Raven.Database.Bundles.Replication.Impl
{
    using System.Linq;

    using Abstractions.Data;
    using Imports.Newtonsoft.Json.Linq;
    using Raven.Json.Linq;

    internal static class Historian
    {
        public static bool IsDirectChildOfCurrent(RavenJObject incomingMetadata, RavenJObject existingMetadata)
        {
            var history = incomingMetadata[Constants.RavenReplicationHistory];
            //if we the source of the metadata we have is the same as the incoming
            //and the incoming has a higher version than we can assume it is a parent
            //since we now merge histories.
            if (incomingMetadata[Constants.RavenReplicationSource].Value<string>() ==
                existingMetadata[Constants.RavenReplicationSource].Value<string>()
                && incomingMetadata[Constants.RavenReplicationVersion].Value<long>()
                >= existingMetadata[Constants.RavenReplicationVersion].Value<long>())
                return true;

            if (history == null || history.Type == JTokenType.Null) // no history, not a parent
                return false;

            if (history.Type != JTokenType.Array)
                return false;

            //Checking that the incoming document contains as a source the same source as the exsisting document source
            // and has a version higher or equal to the exsisting document (since we now merge the replication history).
            return history.Values().Any(x => RavenJTokenEqualityComparer.Default.Equals(
                    ((RavenJObject) x)[Constants.RavenReplicationSource], existingMetadata[Constants.RavenReplicationSource])
                    && ((RavenJObject) x)[Constants.RavenReplicationVersion].Value<long>() >= existingMetadata[Constants.RavenReplicationVersion].Value<long>());            
        }

        public static RavenJArray MergeReplicationHistories(RavenJArray leftHandHistory, RavenJArray rightHandHistory)
        {
            var sourcesToVersionEntries = new Dictionary<string, RavenJObject>();
            MergeSingleHistory(leftHandHistory, sourcesToVersionEntries);
            MergeSingleHistory(rightHandHistory, sourcesToVersionEntries);
            return new RavenJArray(sourcesToVersionEntries.Values); 
        }

        public static void MergeSingleHistory(RavenJArray history, Dictionary<string, RavenJObject> sourcesToVersionEntries)
        {
            foreach (var entry in history.Values())
            {
                var entryAsObject = (RavenJObject) entry;
                var sourceAsString = entryAsObject[Constants.RavenReplicationSource].Value<string>();
                var versionAsLong = entryAsObject[Constants.RavenReplicationVersion].Value<long>();
                RavenJObject val;
                if (!sourcesToVersionEntries.TryGetValue(sourceAsString, out val)
                    || val[Constants.RavenReplicationVersion].Value<long>() < versionAsLong)
                    sourcesToVersionEntries[sourceAsString] = entryAsObject;
            }
        }
    }
}
