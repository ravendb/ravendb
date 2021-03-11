using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Documents.Handlers
{
    public class LegacyReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/replication/lastEtag", "GET", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task LastEtag()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var sourceReplicationDocument = GetSourceReplicationInformation(context, GetRemoteServerInstanceId(), out _);
                var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(sourceReplicationDocument, context);
                context.Write(writer, blittable);
            }
        }

        [RavenAction("/databases/*/replication/replicateDocs", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Documents()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var stream = new ArrayStream(RequestBodyStream(), "Docs"))
            using (var source = new StreamSource(stream, context, Database))
            {
                var destination = new DatabaseDestination(Database);
                var options = new DatabaseSmugglerOptionsServerSide
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    ReadLegacyEtag = true,
#pragma warning restore CS0618 // Type or member is obsolete
                    OperateOnTypes = DatabaseItemType.Documents
                };

                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options);
                var result = await smuggler.ExecuteAsync();

                var replicationSource = GetSourceReplicationInformation(context, GetRemoteServerInstanceId(), out var documentId);
                replicationSource.LastDocumentEtag = result.LegacyLastDocumentEtag;
                replicationSource.Source = GetFromServer();
                replicationSource.LastBatchSize = result.Documents.ReadCount + result.Tombstones.ReadCount;
                replicationSource.LastModified = DateTime.UtcNow;

                await SaveSourceReplicationInformation(replicationSource, context, documentId);
            }
        }

        [RavenAction("/databases/*/replication/replicateAttachments", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Attachments()
        {
            var destination = new DatabaseDestination(Database);
            var options = new DatabaseSmugglerOptionsServerSide
            {
                OperateOnTypes = DatabaseItemType.Attachments,
                SkipRevisionCreation = true
            };

            await using (destination.InitializeAsync(options, null, buildVersion: default))
            await using (var documentActions = destination.Documents())
            using (var buffered = new BufferedStream(RequestBodyStream()))
#pragma warning disable CS0618 // Type or member is obsolete
            using (var reader = new BsonReader(buffered))
#pragma warning restore CS0618 // Type or member is obsolete
            {
                var result = LegacyAttachmentUtils.GetObject(reader);

                const string idProperty = "@id";
                const string etagProperty = "@etag";
                const string metadataProperty = "@metadata";
                const string dataProperty = "data";

                string lastAttachmentEtag = null;
                var progress = new SmugglerProgressBase.CountsWithLastEtagAndAttachments();
                foreach (var attachmentObject in result.Values)
                {
                    if (!(attachmentObject is Dictionary<string, object> attachmentDictionary))
                        throw new InvalidDataException("attachmentObject isn't a Dictionary<string, object>");

                    if (attachmentDictionary.TryGetValue(idProperty, out var attachmentKeyObject) == false)
                        throw new InvalidDataException($"{idProperty} doesn't exist");

                    if (!(attachmentKeyObject is string attachmentKey))
                        throw new InvalidDataException($"{idProperty} isn't of type string");

                    if (attachmentDictionary.TryGetValue(etagProperty, out var lastAttachmentEtagObject) == false)
                        throw new InvalidDataException($"{etagProperty} doesn't exist");

                    if (!(lastAttachmentEtagObject is byte[] lastAttachmentEtagByteArray))
                        throw new InvalidDataException($"{etagProperty} isn't of type byte[]");

                    lastAttachmentEtag = LegacyAttachmentUtils.ByteArrayToEtagString(lastAttachmentEtagByteArray);

                    if (attachmentDictionary.TryGetValue(metadataProperty, out object metadataObject) == false)
                        throw new InvalidDataException($"{metadataProperty} doesn't exist");

                    if (!(metadataObject is Dictionary<string, object> metadata))
                        throw new InvalidDataException($"{idProperty} isn't of type string");

                    if (metadata.TryGetValue("Raven-Delete-Marker", out var deletedObject) && deletedObject is bool deletedObjectAsBool && deletedObjectAsBool)
                    {
                        var id = StreamSource.GetLegacyAttachmentId(attachmentKey);
                        await documentActions.DeleteDocumentAsync(id);
                        continue;
                    }

                    var djv = new DynamicJsonValue();
                    foreach (var keyValue in metadata)
                    {
                        var key = keyValue.Key;
                        if (key.Equals("Raven-Replication-Source") ||
                            key.Equals("Raven-Replication-Version") ||
                            key.Equals("Raven-Replication-History"))
                            continue;

                        djv[key] = keyValue.Value;
                    }

                    var contextToUse = documentActions.GetContextForNewDocument();
                    var metadataBlittable = contextToUse.ReadObject(djv, "metadata");

                    if (attachmentDictionary.TryGetValue(dataProperty, out object dataObject) == false)
                        throw new InvalidDataException($"{dataProperty} doesn't exist");

                    if (!(dataObject is byte[] data))
                        throw new InvalidDataException($"{dataProperty} isn't of type byte[]");

                    using (var dataStream = new MemoryStream(data))
                    {
                        var attachment = new DocumentItem.AttachmentStream
                        {
                            Stream = documentActions.GetTempStream()
                        };

                        var attachmentDetails = StreamSource.GenerateLegacyAttachmentDetails(contextToUse, dataStream, attachmentKey, metadataBlittable, ref attachment);

                        var documentItem = new DocumentItem
                        {
                            Document = new Document
                            {
                                Data = StreamSource.WriteDummyDocumentForAttachment(contextToUse, attachmentDetails),
                                Id = attachmentDetails.Id,
                                ChangeVector = string.Empty,
                                Flags = DocumentFlags.HasAttachments,
                                LastModified = Database.Time.GetUtcNow()
                            },
                            Attachments = new List<DocumentItem.AttachmentStream>
                            {
                                attachment
                            }
                        };

                        await documentActions.WriteDocumentAsync(documentItem, progress);
                    }
                }

                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var replicationSource = GetSourceReplicationInformation(context, GetRemoteServerInstanceId(), out var documentId);
                    replicationSource.LastAttachmentEtag = lastAttachmentEtag;
                    replicationSource.Source = GetFromServer();
                    replicationSource.LastModified = DateTime.UtcNow;

                    await SaveSourceReplicationInformation(replicationSource, context, documentId);
                }
            }
        }

        [RavenAction("/databases/*/replication/heartbeat", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task Heartbeat()
        {
            // nothing to do here
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/last-queried", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task LastQueried()
        {
            // nothing to do here
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/side-by-side-indexes", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task SideBySideIndexes()
        {
            // nothing to do here
            return Task.CompletedTask;
        }

        private Guid GetRemoteServerInstanceId()
        {
            var remoteServerIdString = GetQueryStringValueAndAssertIfSingleAndNotEmpty("dbid");
            return Guid.Parse(remoteServerIdString);
        }

        private string GetFromServer()
        {
            var fromServer = GetQueryStringValueAndAssertIfSingleAndNotEmpty("from");

            if (string.IsNullOrEmpty(fromServer))
                throw new ArgumentException($"from cannot be null or empty", "from");

            while (fromServer.EndsWith("/"))
                fromServer = fromServer.Substring(0, fromServer.Length - 1); // remove last /, because that has special meaning for Raven

            return fromServer;
        }

        private LegacySourceReplicationInformation GetSourceReplicationInformation(DocumentsOperationContext context, Guid remoteServerInstanceId, out string documentId)
        {
            documentId = $"Raven/Replication/Sources/{remoteServerInstanceId}";

            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, documentId);
                if (document == null)
                {
                    return new LegacySourceReplicationInformation
                    {
                        ServerInstanceId = Database.DbId
                    };
                }

                return JsonDeserializationServer.LegacySourceReplicationInformation(document.Data);
            }
        }

        private async Task SaveSourceReplicationInformation(LegacySourceReplicationInformation replicationSource, DocumentsOperationContext context, string documentId)
        {
            var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(replicationSource, context);
            using (var cmd = new MergedPutCommand(blittable, documentId, null, Database))
            {
                await Database.TxMerger.Enqueue(cmd);
            }
        }

        private static class LegacyAttachmentUtils
        {
            public static Dictionary<string, object> GetObject(JsonReader reader)
            {
                if (reader.TokenType == JsonToken.None)
                {
                    if (reader.Read() == false)
                        throw new Exception("Error reading from JsonReader.");
                }

                if (reader.TokenType != JsonToken.StartObject)
                    throw new Exception($"Error reading from JsonReader. Current JsonReader item is not an object: {reader.TokenType}");

                if (reader.Read() == false)
                    throw new Exception("Unexpected end of JSON object");

                string propName = null;
                var result = new Dictionary<string, object>();

                do
                {
                    switch (reader.TokenType)
                    {
                        case JsonToken.Comment:
                            // ignore comments
                            break;

                        case JsonToken.PropertyName:
                            propName = reader.Value.ToString();
                            if (string.Equals(propName, string.Empty))
                                throw new InvalidDataException("Deserializing JSON object with empty string as property name is not supported.");
                            break;

                        case JsonToken.EndObject:
                            return result;

                        case JsonToken.StartObject:
                            if (string.IsNullOrEmpty(propName) == false)
                            {
                                var val = GetObject(reader);
                                result[propName] = val;
                                propName = null;
                            }
                            else
                            {
                                throw new InvalidOperationException($"The JsonReader should not be on a token of type {reader.TokenType}.");
                            }
                            break;

                        case JsonToken.StartArray:
                            if (string.IsNullOrEmpty(propName) == false)
                            {
                                var val = GetArray(reader);
                                result[propName] = val;
                                propName = null;
                            }
                            else
                            {
                                throw new InvalidOperationException($"The JsonReader should not be on a token of type {reader.TokenType}.");
                            }
                            break;

                        default:
                            if (string.IsNullOrEmpty(propName) == false)
                            {
                                var val = GetValue(reader);
                                result[propName] = val;
                                propName = null;
                            }
                            else
                            {
                                throw new InvalidOperationException($"The JsonReader should not be on a token of type {reader.TokenType}.");
                            }
                            break;
                    }
                } while (reader.Read());

                throw new Exception("Error reading from JsonReader.");
            }

            private static object GetValue(JsonReader reader)
            {
                object v;
                switch (reader.TokenType)
                {
                    case JsonToken.String:
                    case JsonToken.Integer:
                    case JsonToken.Float:
                    case JsonToken.Date:
                    case JsonToken.Boolean:
                    case JsonToken.Bytes:
                        v = reader.Value;
                        break;

                    case JsonToken.Null:
                        v = null;
                        break;

                    case JsonToken.Undefined:
                        v = null;
                        break;

                    default:
                        throw new InvalidOperationException($"The JsonReader should not be on a token of type {reader.TokenType}.");
                }
                return v;
            }

            private static List<object> GetArray(JsonReader reader)
            {
                if (reader.TokenType == JsonToken.None)
                {
                    if (!reader.Read())
                        throw new Exception("Error reading from JsonReader.");
                }

                if (reader.TokenType != JsonToken.StartArray)
                    throw new Exception($"Error reading from JsonReader. Current JsonReader item is not an array: {reader.TokenType}");

                if (reader.Read() == false)
                    throw new Exception("Unexpected end of JSON array");

                var ar = new List<object>();
                do
                {
                    switch (reader.TokenType)
                    {
                        case JsonToken.Comment:
                            // ignore comments
                            break;

                        case JsonToken.EndArray:
                            return ar;

                        case JsonToken.StartObject:
                            ar.Add(GetObject(reader));
                            break;

                        case JsonToken.StartArray:
                            ar.Add(GetArray(reader));
                            break;

                        default:
                            ar.Add(GetValue(reader));
                            break;
                    }
                } while (reader.Read());

                throw new Exception("Error reading an array from JsonReader.");
            }

            private static readonly int[] ByteToHexStringAsInt32Lookup;

            static LegacyAttachmentUtils()
            {
                ByteToHexStringAsInt32Lookup = new int[256];
                var abcdef = "0123456789ABCDEF";
                for (var i = 0; i < 256; i++)
                {
                    var hex = (abcdef[i / 16] | (abcdef[i % 16] << 16));
                    ByteToHexStringAsInt32Lookup[i] = hex;
                }
            }

            public static unsafe string ByteArrayToEtagString(byte[] byteArray)
            {
                var results = new string('-', 36);

                fixed (byte* restarts = byteArray)
                fixed (char* buf = results)
                {
                    int fst = (*restarts << 24) | (*(restarts + 1) << 16) | (*(restarts + 2) << 8) | (*(restarts + 3));
                    int snd = (*(restarts + 4) << 24) | (*(restarts + 5) << 16) | (*(restarts + 6) << 8) | (*(restarts + 7));
                    var etagRestarts = (uint)snd | ((long)fst << 32);

                    var changes = restarts + 8;

                    fst = (*changes << 24) | (*(changes + 1) << 16) | (*(changes + 2) << 8) | (*(changes + 3));
                    snd = (*(changes + 4) << 24) | (*(changes + 5) << 16) | (*(changes + 6) << 8) | (*(changes + 7));
                    var etagChanges = (uint)snd | ((long)fst << 32);

                    var bytes = new LongBytes { Long = etagRestarts };

                    *(int*)(&buf[0]) = ByteToHexStringAsInt32Lookup[bytes.Byte7];
                    *(int*)(&buf[2]) = ByteToHexStringAsInt32Lookup[bytes.Byte6];
                    *(int*)(&buf[4]) = ByteToHexStringAsInt32Lookup[bytes.Byte5];
                    *(int*)(&buf[6]) = ByteToHexStringAsInt32Lookup[bytes.Byte4];

                    //buf[8] = '-';
                    *(int*)(&buf[9]) = ByteToHexStringAsInt32Lookup[bytes.Byte3];
                    *(int*)(&buf[11]) = ByteToHexStringAsInt32Lookup[bytes.Byte2];

                    //buf[13] = '-';
                    *(int*)(&buf[14]) = ByteToHexStringAsInt32Lookup[bytes.Byte1];
                    *(int*)(&buf[16]) = ByteToHexStringAsInt32Lookup[bytes.Byte0];

                    //buf[18] = '-';

                    bytes.Long = etagChanges;

                    *(int*)(&buf[19]) = ByteToHexStringAsInt32Lookup[bytes.Byte7];
                    *(int*)(&buf[21]) = ByteToHexStringAsInt32Lookup[bytes.Byte6];

                    //buf[23] = '-';
                    *(int*)(&buf[24]) = ByteToHexStringAsInt32Lookup[bytes.Byte5];
                    *(int*)(&buf[26]) = ByteToHexStringAsInt32Lookup[bytes.Byte4];
                    *(int*)(&buf[28]) = ByteToHexStringAsInt32Lookup[bytes.Byte3];
                    *(int*)(&buf[30]) = ByteToHexStringAsInt32Lookup[bytes.Byte2];
                    *(int*)(&buf[32]) = ByteToHexStringAsInt32Lookup[bytes.Byte1];
                    *(int*)(&buf[34]) = ByteToHexStringAsInt32Lookup[bytes.Byte0];

                    return results;
                }
            }

            [StructLayout(LayoutKind.Explicit)]
            private struct LongBytes
            {
                [FieldOffset(0)]
                public long Long;

                [FieldOffset(0)]
                public byte Byte0;

                [FieldOffset(1)]
                public byte Byte1;

                [FieldOffset(2)]
                public byte Byte2;

                [FieldOffset(3)]
                public byte Byte3;

                [FieldOffset(4)]
                public byte Byte4;

                [FieldOffset(5)]
                public byte Byte5;

                [FieldOffset(6)]
                public byte Byte6;

                [FieldOffset(7)]
                public byte Byte7;
            }
        }
    }

    public class LegacySourceReplicationInformation
    {
        public LegacySourceReplicationInformation()
        {
            LastDocumentEtag = LastEtagsInfo.EtagEmpty;
            LastAttachmentEtag = LastEtagsInfo.EtagEmpty;
        }

        public string LastDocumentEtag { get; set; }

        public string LastAttachmentEtag { get; set; }

        public Guid ServerInstanceId { get; set; }

        public string Source { get; set; }

        public DateTime? LastModified { get; set; }

        public long LastBatchSize { get; set; }
    }
}
