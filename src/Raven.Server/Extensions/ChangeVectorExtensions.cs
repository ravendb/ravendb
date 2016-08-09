using System;
using System.Collections.Generic;
using System.Text;
using Raven.Abstractions.Replication;

namespace Raven.Server.Extensions
{
    public static class ChangeVectorExtensions
    {
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

        public static void UpdateChangeVectorFrom(this ChangeVectorEntry[] changeVector, Dictionary<Guid, long> maxEtagsPerDbId)
        {
            for (int i = 0; i < changeVector.Length; i++)
            {
                long dbEtag;
                if (maxEtagsPerDbId.TryGetValue(changeVector[i].DbId, out dbEtag) == false)
                    continue;
                maxEtagsPerDbId.Remove(changeVector[i].DbId);
                if (dbEtag > changeVector[i].Etag)
                {
                    changeVector[i].Etag = dbEtag;
                }
            }
       
            if (maxEtagsPerDbId.Count <= 0)
                return;

            var oldSize = changeVector.Length;
            Array.Resize(ref changeVector, oldSize + maxEtagsPerDbId.Count);

            foreach (var kvp in maxEtagsPerDbId)
            {
                changeVector[oldSize++] = new ChangeVectorEntry
                {
                    DbId = kvp.Key,
                    Etag = kvp.Value,
                };
            }
        }
    }
}
