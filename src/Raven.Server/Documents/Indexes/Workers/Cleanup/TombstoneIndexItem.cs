using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public class TombstoneIndexItem : IDisposable
    {
        public IndexItemType Type;
        public LazyStringValue LowerId;
        public LazyStringValue PrefixKey;
        public LazyStringValue Name;
        public long Etag;
        public DateTime From;
        public DateTime To;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LowerId)] = LowerId?.ToString(),
                [nameof(PrefixKey)] = PrefixKey?.ToString(),
                [nameof(Name)] = Name?.ToString(),
                [nameof(Etag)] = Etag,
                [nameof(Type)] = Type.GetDescription(),
                [nameof(From)] = From.ToString(),
                [nameof(To)] = To.ToString(),
            };
        }

        public static Tombstone DocumentTombstoneIndexItemToTombstone(DocumentsOperationContext context, TombstoneIndexItem tombstoneIndexItem)
        {
            return new Tombstone
            {
                Type = Tombstone.TombstoneType.Document, 
                LowerId = tombstoneIndexItem.LowerId.Clone(context), 
                Etag = tombstoneIndexItem.Etag
            };
        }

        public void Dispose()
        {
            LowerId?.Dispose();
            PrefixKey?.Dispose();
            Name?.Dispose();
        }
    }
}
