using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class Tombstone
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
            Revision = 3
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                ["Id"] = LowerId.ToString(),
                [nameof(Etag)] = Etag,
                [nameof(DeletedEtag)] = DeletedEtag,
                [nameof(Type)] = Type.ToString(),
                [nameof(ChangeVector)] = ChangeVector
            };

            if (Type != TombstoneType.Attachment)
            {
                json[nameof(Collection)] = Collection.ToString();
            }

            return json;
        }
    }
}
