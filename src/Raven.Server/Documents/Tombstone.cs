using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class Tombstone : IDisposable
    {
        public long StorageId;

        public TombstoneType Type;
        public LazyStringValue LowerId;

        public long Etag;
        public long DeletedEtag;
        public short TransactionMarker;

        #region Document

        public LazyStringValue Collection;
        public DocumentFlags Flags;

        public string ChangeVector;
        public DateTime LastModified;

        #endregion

        public enum TombstoneType : byte
        {
            Document = 1,
            Attachment = 2,
            Revision = 3,
            Counter = 4
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                ["Id"] = LowerId.ToString(),
                [nameof(Etag)] = Etag,
                [nameof(DeletedEtag)] = DeletedEtag,
                [nameof(Type)] = Type.ToString(),
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(LastModified)] = LastModified
            };

            if (Type != TombstoneType.Attachment)
            {
                json[nameof(Collection)] = Collection.ToString();
            }

            return json;
        }

        public static Tombstone FromJson(JsonOperationContext ctx, BlittableJsonReaderObject json)
        {
            if (json == null)
                return null;

            json.TryGet(nameof(ChangeVector), out string changeVector);
            json.TryGet("Id", out string lowerId);
            json.TryGet(nameof(Etag), out long etag);
            json.TryGet(nameof(DeletedEtag), out long deletedEtag);
            json.TryGet(nameof(LastModified), out DateTime lastModified);
            json.TryGet(nameof(Type), out string typeStr);
            Enum.TryParse(typeStr, out TombstoneType type);
           
            var tombstone =  new Tombstone
            {
                ChangeVector = changeVector,
                LowerId = ctx.GetLazyStringForFieldWithCaching(lowerId),
                Etag = etag,
                DeletedEtag = deletedEtag,
                LastModified = lastModified,
                Type = type
            };

            if (type != TombstoneType.Attachment)
            {
                json.TryGet(nameof(Collection), out string collection);
                tombstone.Collection = ctx.GetLazyStringForFieldWithCaching(collection);
            }

            return tombstone;
        }

        public void Dispose()
        {
            LowerId?.Dispose();
            Collection?.Dispose();
        }
    }
}
