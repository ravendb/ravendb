using System.IO;
using System.Text;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public static class ChangeVectorExtensions
    {              
        public static string Format(this ChangeVectorEntry[] changeVector, int? maxCount = null)
        {
            var max = maxCount ?? changeVector.Length;
            if (max == 0)
                return "[]";
            var sb = new StringBuilder();
            sb.Append("[");
            for (var i = 0; i < max; i++)
            {
                sb.Append(changeVector[i].DbId)
                    .Append(" : ")
                    .Append(changeVector[i].Etag)
                    .Append(", ");
            }
            sb.Length -= 2;
            sb.Append("]");
            return sb.ToString();
        }

        public static ChangeVectorEntry[] ToVector(this BlittableJsonReaderArray vectorJson)
        {
            var result = new ChangeVectorEntry[vectorJson.Length];
            for (var i = 0; i < vectorJson.Length; i++)
            {
                var entryJson  = (BlittableJsonReaderObject)vectorJson[i];
                if (entryJson.TryGet(nameof(ChangeVectorEntry.DbId), out result[i].DbId) == false)
                    throw new InvalidDataException("Tried to find " + nameof(ChangeVectorEntry.DbId) + " property in change vector, but didn't find.");
                if (entryJson.TryGet(nameof(ChangeVectorEntry.Etag), out result[i].Etag) == false)
                    throw new InvalidDataException("Tried to find " + nameof(ChangeVectorEntry.Etag) + " property in change vector, but didn't find.");
            }
            return result;
        }

        public static string ToJson(this ChangeVectorEntry[] self)
        {
            if (self == null)
                return null;

            var sb = new StringBuilder();
            for (int i = 0; i < self.Length; i++)
            {
                if (i != 0)
                    sb.Append(", ");
                self[i].Append(sb);
            }
            return sb.ToString();
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
            //TODO: validation of valid chars
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