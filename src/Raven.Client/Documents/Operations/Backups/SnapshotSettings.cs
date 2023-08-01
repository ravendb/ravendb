using System.IO.Compression;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public sealed class SnapshotSettings : IDynamicJson
    {
        public CompressionLevel CompressionLevel { get; set; }

        public bool ExcludeIndexes { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CompressionLevel)] = CompressionLevel,
                [nameof(ExcludeIndexes)] = ExcludeIndexes
            };
        }

        public DynamicJsonValue ToAuditJson()
        {
           return ToJson();
        }
    }
}
