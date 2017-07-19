using System.IO;
using System.Text;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;

namespace Raven.Client.Extensions
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
    }
}