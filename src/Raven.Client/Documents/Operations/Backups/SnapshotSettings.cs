using System.IO.Compression;
using Sparrow.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class SnapshotSettings : IDynamicJson
    {
        public SnapshotBackupCompressionAlgorithm? CompressionAlgorithm { get; set; }

        public CompressionLevel CompressionLevel { get; set; }

        public bool ExcludeIndexes { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CompressionAlgorithm)] = CompressionAlgorithm,
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
