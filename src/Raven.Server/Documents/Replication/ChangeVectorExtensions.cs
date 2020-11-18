using System;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.Replication
{
    public static class ChangeVectorExtensions
    {
        /// <summary>
        /// Generate DbId that is then can be put in the ChangeVectorEntry DbId field
        /// </summary>
        public static unsafe string AsChangeVectorDbId(this Guid DbId)
        {
            var dbIdAsString = new string(' ', 22);
            fixed (char* dbIdPtr = dbIdAsString)
            {
                var res = Base64.ConvertToBase64ArrayUnpadded(dbIdPtr, (byte*)&DbId, 0, 16);
                Debug.Assert(res == 22);
            }

            return dbIdAsString;
        }

        public static string SerializeVector(this ChangeVectorEntry[] self)
        {
            if (self == null)
                return null;

            self = self.OrderBy(x => x.DbId, StringComparer.OrdinalIgnoreCase).ToArray();

            var sb = new StringBuilder();
            for (int i = 0; i < self.Length; i++)
            {
                if (i != 0)
                    sb.Append(", ");
                self[i].Append(sb);
            }
            return sb.ToString();
        }        

        public static string SerializeVector(this List<ChangeVectorEntry> self)
        {
            return SerializeVector(self.ToArray());
        }

        public static void ToBase26(StringBuilder sb, int tag)
        {
            do
            {
                var reminder = tag % 26;
                sb.Append((char)('A' + reminder));
                tag /= 26;
            } while (tag != 0);
        }

        public static int FromBase26(string tag)
        {
            var val = 0;
            for (int i = 0; i < tag.Length; i++)
            {
                val *= 26;
                val += (tag[i] - 'A');
            }
            return val;
        }
    }
}
