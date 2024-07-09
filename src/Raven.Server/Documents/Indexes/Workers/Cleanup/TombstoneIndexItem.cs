using System;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public class TombstoneIndexItem : IDisposable
    {
        public IndexItemType Type;
        public LazyStringValue LowerId;
        public LazyStringValue LuceneKey;
        public LazyStringValue Name;
        public long Etag;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LowerId)] = LowerId?.ToString(),
                [nameof(LuceneKey)] = LuceneKey?.ToString(),
                [nameof(Name)] = Name?.ToString(),
                [nameof(Etag)] = Etag,
                [nameof(Type)] = Type.GetDescription(),

            };
        }

        public void Dispose()
        {
            LowerId?.Dispose();
            LuceneKey?.Dispose();
            Name?.Dispose();
        }
    }
}
