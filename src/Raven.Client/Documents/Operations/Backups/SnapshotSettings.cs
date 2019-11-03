using System.IO.Compression;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class SnapshotSettings : IDynamicJson
    {
        public CompressionLevel CompressionLevel { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CompressionLevel)] = CompressionLevel
            };
        }
    }
}
