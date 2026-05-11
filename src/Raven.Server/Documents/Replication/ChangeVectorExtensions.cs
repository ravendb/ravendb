using System;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data.HashFunction.Blake2;
using Sparrow.Server;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.Replication
{
    public static class ChangeVectorExtensions
    {
        private const int ChangeVectorDbIdSizeInBytes = 16;

        /// <summary>
        /// Generate DbId that is then can be put in the ChangeVectorEntry DbId field
        /// </summary>
        public static unsafe string AsChangeVectorDbId(this Guid DbId)
        {
            var dbIdAsString = new string(' ', 22);
            var res = Base64.ConvertToBase64ArrayUnpadded(dbIdAsString, (byte*)&DbId, 0, 16);
            Debug.Assert(res == 22);

            return dbIdAsString;
        }

        public static string DeriveReplicationMarkerDbId(this Guid dbId, string markerTag)
        {
            if (string.IsNullOrEmpty(markerTag))
                throw new ArgumentException("Marker tag cannot be null or empty.", nameof(markerTag));

            Span<byte> dbIdBytes = stackalloc byte[ChangeVectorDbIdSizeInBytes];
            if (dbId.TryWriteBytes(dbIdBytes) == false)
                throw new ArgumentException("Unable to write database id bytes.", nameof(dbId));

            var markerBytes = Encoding.UTF8.GetBytes(markerTag);
            var markerAndDbIdBytes = new byte[markerBytes.Length + 1 + dbIdBytes.Length];

            markerBytes.CopyTo(markerAndDbIdBytes, 0);
            markerAndDbIdBytes[markerBytes.Length] = 0;
            dbIdBytes.CopyTo(markerAndDbIdBytes.AsSpan(markerBytes.Length + 1));

            var hash = Blake2BFactory.Instance
                .Create(new Blake2BConfig { HashSizeInBits = ChangeVectorDbIdSizeInBytes * 8 }) // Bytes to Bits
                .ComputeHash(markerAndDbIdBytes)
                .Hash;
            Debug.Assert(hash.Length == ChangeVectorDbIdSizeInBytes);
            
            return Format.ToBase64Unpadded(hash);
        }

        public static string SerializeVector(this ChangeVectorEntry[] self)
        {
            if (self == null)
                return null;

            Array.Sort(self, (x, y) => string.CompareOrdinal(x.DbId, y.DbId));
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
            if (self == null)
                return null;

            self.Sort((x, y) => string.CompareOrdinal(x.DbId, y.DbId));
            var sb = new StringBuilder();
            for (int i = 0; i < self.Count; i++)
            {
                if (i != 0)
                    sb.Append(", ");
                self[i].Append(sb);
            }
            return sb.ToString();
        }

        public static void ToBase26(StringBuilder sb, int tag)
        {
            while (true)
            {
                var reminder = tag % 26;
                sb.Append((char)('A' + reminder));
                tag /= 26;

                if (tag == 0)
                    break;

                tag--;
            }
        }

        public static int FromBase26(string tag) => tag.ParseNodeTag();
    }
}
