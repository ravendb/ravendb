using System;
using Newtonsoft.Json.Linq;
using Raven.Database;

namespace Raven.Bundles.Replication
{
    public class ReplicationUtil
    {
        public static void AddAncestry(Guid? oldEtag, JObject metadata)
        {
            var ancestry = metadata.Value<JArray>(ReplicationConstants.RavenAncestry);
            if (ancestry == null)
            {
                ancestry = new JArray();
                metadata.Add(ReplicationConstants.RavenAncestry, ancestry);
            }
            if (oldEtag != null)
                ancestry.Add(JToken.FromObject(oldEtag.ToString()));
            if (ancestry.Count > 15)
                ancestry.RemoveAt(0);
        }
    }
}