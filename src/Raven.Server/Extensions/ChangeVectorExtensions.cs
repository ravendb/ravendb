using System;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Extensions
{
    public static class ChangeVectorExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Dictionary<Guid, long> ToDictionary(this ChangeVectorEntry[] changeVector)
        {
            return changeVector.ToDictionary(x => x.DbId, x => x.Etag);
        }


        //note - this is a helper to use in unit tests only
        public static ChangeVectorEntry FromJson(this RavenJToken self)
        {
            return new ChangeVectorEntry
            {
                DbId = Guid.Parse(self.Value<string>("DbId")),
                Etag = long.Parse(self.Value<string>("Etag"))
            };
        }

        public static DynamicJsonArray ToJson(this ChangeVectorEntry[] self)
        {
            var results = new DynamicJsonArray();
            foreach(var entry in self)
                results.Add(new DynamicJsonValue
                {
                    ["DbId"] = entry.DbId.ToString(),
                    ["Etag"] = entry.Etag
                });
            return results;
        }


        public static bool GreaterThen(this ChangeVectorEntry[] self, Dictionary<Guid,long> other)
        {
            for (int i = 0; i < self.Length; i++)
            {
                long otherEtag;
                if (other.TryGetValue(self[i].DbId, out otherEtag) == false)
                    return true;
                if (self[i].Etag > otherEtag)
                    return true;
            }
            return false;
        }	

        public static string Format(this ChangeVectorEntry[] changeVector)
        {
            if (changeVector.Length == 0)
                return "[]";
            var sb = new StringBuilder();
            sb.Append("[");
            for (int i = 0; i < changeVector.Length; i++)
            {
                sb.Append(changeVector[i].DbId)
                    .Append(" : ")
                    .Append(changeVector[i].Etag)
                    .Append(", ");
            }
            sb.Length -= 3;
            sb.Append("]");
            return sb.ToString();
        }
    }
}
