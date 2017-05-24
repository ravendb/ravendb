using System;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class DocumentTombstone
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

        public ChangeVectorEntry[] ChangeVector;
        public DateTime LastModified;

        #endregion

        public enum TombstoneType : byte
        {
            Document = 1,
            Attachment = 2,
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                ["Id"] = LowerId.ToString(),
                [nameof(Etag)] = Etag,
                [nameof(DeletedEtag)] = DeletedEtag,
                [nameof(Type)] = Type.ToString(),
            };

            if (Type == TombstoneType.Document)
            {
                json[nameof(Collection)] = Collection.ToString();
                json[nameof(ChangeVector)] = ChangeVector.ToString();
            }

            return json;
        }
    }
}