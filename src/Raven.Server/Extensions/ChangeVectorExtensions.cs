using System;
using System.Collections.Generic;
using System.Text;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;

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
    }
}
