using System;
using System.Text;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public unsafe class BlittableMetadataModifierTest(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void BlittableMetadataModifier_WhileIdContainsNoEscapeCharacters_ResultInLazyStringWithoutEscapeInformation()
        {
            const string json = "{\"@metadata\": { \"@id\": \"u1\"}}";

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var buffer = Encoding.UTF8.GetBytes(json);
                var state = new JsonParserState();

                var modifier = new BlittableMetadataModifier(ctx);
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                    fixed (byte* pBuffer = buffer)
                    {
                        parser.SetBuffer(pBuffer, buffer.Length);

                        using (
                            var builder = new BlittableJsonDocumentBuilder(ctx,
                                BlittableJsonDocumentBuilder.UsageMode.None, "test", parser, state, null, modifier))
                        {
                            builder.ReadObjectDocument();
                            builder.Read();
                            builder.FinalizeDocument();
                        }
                    }

                Assert.NotNull(modifier.Id.EscapePositions);
            }
        }


        [RavenFact(RavenTestCategory.Core)]
        public void ShouldSkipAllLegacyMetadataPropertiesForLegacyImport()
        {
            const string json = """
            {
                "@metadata": {
                    "@id": "orders/1",
                    "Last-Modified": "2024-01-01T00:00:00.0000000",
                    "Raven-Read-Only": "true",
                    "Raven-Entity-Name": "Orders",
                    "Raven-Last-Modified": "2024-01-01T00:00:00.0000000",
                    "Raven-Delete-Marker": false,
                    "Raven-Expiration-Date": "2099-01-01T00:00:00.0000000",
                    "Raven-Document-Revision": "1",
                    "Raven-Replication-Source": "some-source",
                    "Raven-Replication-Merged-History": "true",
                    "Non-Authoritative-Information": "false",
                    "Raven-Document-Parent-Revision": "0",
                    "Raven-Document-Revision-Status": "Current",
                    "Raven-Replication-Version": "1",
                    "Raven-Replication-History": []
                }
            }
            """;

            using var ctx = JsonOperationContext.ShortTermSingleUse();

            var buffer = Encoding.UTF8.GetBytes(json);
            var state = new JsonParserState();

            var modifier = new BlittableMetadataModifier(ctx, legacyImport: true, readLegacyEtag: false, operateOnTypes: DatabaseItemType.Documents);

            BlittableJsonReaderObject result;
            using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
            {
                fixed (byte* pBuffer = buffer)
                {
                    parser.SetBuffer(pBuffer, buffer.Length);
                    using var builder = new BlittableJsonDocumentBuilder(ctx, BlittableJsonDocumentBuilder.UsageMode.None, "test", parser, state, null, modifier);
                    builder.ReadObjectDocument();
                    builder.Read();
                    builder.FinalizeDocument();
                    result = builder.CreateReader();
                }
            }

            using (result)
            {
                Assert.True(result.TryGet("@metadata", out BlittableJsonReaderObject metadata));
                Assert.NotNull(metadata);

                Assert.False(metadata.TryGet("Last-Modified", out object _), "Last-Modified should be stripped");
                Assert.False(metadata.TryGet("Raven-Read-Only", out string _), "Raven-Read-Only should be stripped");
                Assert.False(metadata.TryGet("Raven-Entity-Name", out string _), "Raven-Entity-Name should be stripped");
                Assert.False(metadata.TryGet("Raven-Last-Modified", out string _), "Raven-Last-Modified should be stripped");
                Assert.False(metadata.TryGet("Raven-Delete-Marker", out bool _), "Raven-Delete-Marker should be stripped");
                Assert.False(metadata.TryGet("Raven-Expiration-Date", out DateTime _), "Raven-Expiration-Date should be stripped");
                Assert.False(metadata.TryGet("Raven-Document-Revision", out string _), "Raven-Document-Revision should be stripped");
                Assert.False(metadata.TryGet("Raven-Replication-Source", out string _), "Raven-Replication-Source should be stripped");
                Assert.False(metadata.TryGet("Non-Authoritative-Information", out string _), "Non-Authoritative-Information should be stripped");
                Assert.False(metadata.TryGet("Raven-Document-Parent-Revision", out string _), "Raven-Document-Parent-Revision should be stripped");
                Assert.False(metadata.TryGet("Raven-Document-Revision-Status", out string _), "Raven-Document-Revision-Status should be stripped");
                Assert.False(metadata.TryGet("Raven-Replication-Merged-History", out string _), "Raven-Replication-Merged-History should be stripped");
                Assert.False(metadata.TryGet("Raven-Replication-Version", out string _), "Raven-Replication-Version should be stripped");
                Assert.False(metadata.TryGet("Raven-Replication-History", out BlittableJsonReaderArray _), "Raven-Replication-History should be stripped");

                Assert.True(metadata.TryGet("@collection", out string _));
                Assert.True(metadata.TryGet("@expires", out DateTime _));

                // @id should still be parsed
                Assert.Equal("orders/1", modifier.Id.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ShouldKeepAllLegacyMetadataProperties()
        {
            const string json = """
            {
                "@metadata": {
                    "@id": "orders/1",
                    "Last-Modified": "2024-01-01T00:00:00.0000000",
                    "Raven-Read-Only": "true",
                    "Raven-Entity-Name": "Orders",
                    "Raven-Last-Modified": "2024-01-01T00:00:00.0000000",
                    "Raven-Delete-Marker": false,
                    "Raven-Expiration-Date": "2099-01-01T00:00:00.0000000",
                    "Raven-Document-Revision": "1",
                    "Raven-Replication-Source": "some-source",
                    "Raven-Replication-Merged-History": "true",
                    "Non-Authoritative-Information": "false",
                    "Raven-Document-Parent-Revision": "0",
                    "Raven-Document-Revision-Status": "Current",
                    "Raven-Replication-Version": "1",
                    "Raven-Replication-History": []
                }
            }
            """;

            using var ctx = JsonOperationContext.ShortTermSingleUse();

            var buffer = Encoding.UTF8.GetBytes(json);
            var state = new JsonParserState();

            var modifier = new BlittableMetadataModifier(ctx, legacyImport: false, readLegacyEtag: false, operateOnTypes: DatabaseItemType.Documents);

            BlittableJsonReaderObject result;
            using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
            {
                fixed (byte* pBuffer = buffer)
                {
                    parser.SetBuffer(pBuffer, buffer.Length);
                    using var builder = new BlittableJsonDocumentBuilder(ctx, BlittableJsonDocumentBuilder.UsageMode.None, "test", parser, state, null, modifier);
                    builder.ReadObjectDocument();
                    builder.Read();
                    builder.FinalizeDocument();
                    result = builder.CreateReader();
                }
            }

            using (result)
            {
                Assert.True(result.TryGet("@metadata", out BlittableJsonReaderObject metadata));
                Assert.NotNull(metadata);

                Assert.True(metadata.TryGet("Last-Modified", out object _));
                Assert.True(metadata.TryGet("Raven-Read-Only", out string _));
                Assert.True(metadata.TryGet("Raven-Entity-Name", out string _));
                Assert.True(metadata.TryGet("Raven-Last-Modified", out string _));
                Assert.True(metadata.TryGet("Raven-Delete-Marker", out bool _));
                Assert.True(metadata.TryGet("Raven-Expiration-Date", out DateTime _));
                Assert.True(metadata.TryGet("Raven-Document-Revision", out string _));
                Assert.True(metadata.TryGet("Raven-Replication-Source", out string _));
                Assert.True(metadata.TryGet("Non-Authoritative-Information", out string _));
                Assert.True(metadata.TryGet("Raven-Document-Parent-Revision", out string _));
                Assert.True(metadata.TryGet("Raven-Document-Revision-Status", out string _));
                Assert.True(metadata.TryGet("Raven-Replication-Merged-History", out string _));
                Assert.True(metadata.TryGet("Raven-Replication-Version", out string _));
                Assert.True(metadata.TryGet("Raven-Replication-History", out BlittableJsonReaderArray _));

                Assert.False(metadata.TryGet("@collection", out string _));
                Assert.False(metadata.TryGet("@expires", out DateTime _));
            }
        }
    }
}
