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
    }
}
