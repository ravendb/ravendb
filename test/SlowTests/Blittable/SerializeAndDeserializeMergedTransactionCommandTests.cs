using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.Util;
using Newtonsoft.Json;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Json.Converters;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Blittable
{
    public class SerializeAndDeserializeMergedTransactionCommandTests : RavenLowLevelTestBase
    {
        public SerializeAndDeserializeMergedTransactionCommandTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SerializeAndDeserialize_PutResolvedConflictsCommand()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var database = CreateDocumentDatabase())
            {
                //Arrange
                var resolvedConflicts = new List<(DocumentConflict, long, bool)>
                    {
                        (new DocumentConflict
                        {
                            Id = context.GetLazyString("Some id"),
                            LowerId = context.GetLazyString("Some lower id"),
                            Collection = context.GetLazyString("Some collection"),
                            Doc = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new { SomeName = "Some Value" }, context)
                        }, 10, false)
                    };

                var expected = new ResolveConflictOnReplicationConfigurationChange.PutResolvedConflictsCommand(
                    null, resolvedConflicts, null);
                var expectedDto = expected.ToDto(context);

                //Serialize
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    jsonSerializer.Serialize(writer, expectedDto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }

                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                //Deserialize
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var actualDto = jsonSerializer.Deserialize<PutResolvedConflictsCommandDto>(reader);

                    //Assert
                    //                    Assert.Equal(expectedDto.)
                }
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_MergedDeleteAttachmentCommand()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                //Arrange
                var changeVector = context.GetLazyString("Some Lazy String");
                var expected = new AttachmentHandler.MergedDeleteAttachmentCommand
                {
                    DocumentId = "someId",
                    Name = "someName",
                    ExpectedChangeVector = changeVector
                };

                //Serialize
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }

                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                //Deserialize
                AttachmentHandler.MergedDeleteAttachmentCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedDeleteAttachmentCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<AttachmentHandler.MergedDeleteAttachmentCommand>(context));
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_MergedPutAttachmentCommand()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var database = CreateDocumentDatabase())
            {
                //Arrange
                var recordFilePath = NewDataPath();

                var attachmentStream = new StreamsTempFile(recordFilePath, database.DocumentsStorage.Environment);
                var stream = attachmentStream.StartNewStream();
                const string bufferContent = "Menahem";
                var buffer = Encoding.ASCII.GetBytes(bufferContent);
                stream.Write(buffer);

                var changeVector = context.GetLazyString("Some Lazy String");
                var expected = new AttachmentHandler.MergedPutAttachmentCommand
                {
                    DocumentId = "someId",
                    Name = "someName",
                    ExpectedChangeVector = changeVector,
                    ContentType = "someContentType",
                    Stream = stream,
                    Hash = "someHash",
                };

                //Serialize
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }

                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                //Deserialize
                AttachmentHandler.MergedPutAttachmentCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedPutAttachmentCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual,
                    new CustomComparer<AttachmentHandler.MergedPutAttachmentCommand>(context, new[] { typeof(Stream) }));

                stream.Seek(0, SeekOrigin.Begin);
                var expectedStream = expected.Stream.ReadData();
                var actualStream = actual.Stream.ReadData();
                Assert.Equal(expectedStream, actualStream);
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_MergedPutCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var document = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                var changeVector = context.GetLazyString("Some Lazy String");
                var expected = new MergedPutCommand(document, "user/", changeVector, null);

                //Action
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                MergedPutCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedPutCommand.MergedPutCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<MergedPutCommand>(context));
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_PatchDocumentCommandTest()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var arg = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                var patchRequest = new PatchRequest("", PatchRequestType.None);

                var expected = new PatchDocumentCommand(
                    context,
                    "Some Id",
                    context.GetLazyString("Some Lazy String"),
                    false,
                    (patchRequest, arg),
                    (null, null),
                    database,
                    false, false, false, false);

                //Action
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);
                PatchDocumentCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<PatchDocumentCommandDto>(reader);
                    actual = dto.ToCommand(context, database);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<PatchDocumentCommand>(context));
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_DeleteDocumentCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                //Arrange
                var expected = new DeleteDocumentCommand("Some Id", "Some Change Vector", null);

                //Action
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                DeleteDocumentCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<DeleteDocumentCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<DeleteDocumentCommand>(context));
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_MergedBatchCommandCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var commands = new[]
                {
                    new BatchRequestParser.CommandData
                    {
                        Id = "Some Id",
                        ChangeVector = context.GetLazyString("Some Lazy String"),
                        Document = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context),
                        Patch = new PatchRequest("Some Script", PatchRequestType.None)
                    }
                };
                var expected = new BatchHandler.MergedBatchCommand(null)
                {
                    ParsedCommands = commands
                };

                //Action
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                BatchHandler.MergedBatchCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedBatchCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<BatchHandler.MergedBatchCommand>(context));
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_MergedBatchPutCommandTest()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var expected = new DatabaseDestination.MergedBatchPutCommand(database, BuildVersionType.V4, null);
                var document = new DocumentItem
                {
                    Document = new Document
                    {
                        ChangeVector = "Some Change Vector",
                        Data = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context),
                        Id = context.GetLazyString("Some Id")
                    }
                };
                expected.Add(document);

                //Action
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                DatabaseDestination.MergedBatchPutCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<DatabaseDestination.MergedBatchPutCommandDto>(reader);
                    actual = dto.ToCommand(context, database);
                }

                //Assert
                var expectedDoc = expected.Documents[0].Document;
                var actualDoc = actual.Documents[0].Document;

                Assert.Equal(expectedDoc.Id, actualDoc.Id);
                Assert.Equal(expectedDoc.ChangeVector, actualDoc.ChangeVector);
                Assert.Equal(expectedDoc.Data, actualDoc.Data);
            }
        }

        [Fact]
        public async Task SerializeAndDeserialize_MergedInsertBulkCommandTest()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var commands = new[]
                {
                    new BatchRequestParser.CommandData
                    {
                        Type = CommandType.PUT,
                        Id = "Some Id",
                        ChangeVector = context.GetLazyString("Some Lazy String"),
                        Document = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context),
                        Patch = new PatchRequest("Some Script", PatchRequestType.None)
                    }
                };
                var expected = new BulkInsertHandler.MergedInsertBulkCommand
                {
                    NumberOfCommands = commands.Length,
                    Commands = commands
                };

                //Action
                var jsonSerializer = GetJsonSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto(context);
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                BulkInsertHandler.MergedInsertBulkCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedInsertBulkCommandDto>(reader);
                    actual = dto.ToCommand(context, database);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<BulkInsertHandler.MergedInsertBulkCommand>(context));
            }
        }

        private static JsonSerializer GetJsonSerializer()
        {
            var jsonSerializer = (JsonSerializer)DocumentConventions.Default.Serialization.CreateSerializer();
            jsonSerializer.Converters.Add(Raven.Server.Json.Converters.BlittableJsonConverter.Instance);
            jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
            jsonSerializer.Converters.Add(StreamConverter.Instance);
            return jsonSerializer;
        }

        private class CustomComparer<T> : IEqualityComparer<T> where T : TransactionOperationsMerger.IRecordableCommand
        {
            private readonly IEnumerable<Type> _notCheckTypes;
            private readonly JsonOperationContext _context;

            public CustomComparer(JsonOperationContext context) : this(context, Array.Empty<Type>())
            {
            }

            public CustomComparer(JsonOperationContext context, IEnumerable<Type> notCheckTypes)
            {
                _context = context;
                _notCheckTypes = notCheckTypes;
            }

            public bool Equals(T expected, T actual)
            {
                var expectedDot = expected.ToDto(_context);
                var actualDot = actual.ToDto(_context);

                var type = actualDot.GetType();

                var props = type.GetProperties();
                foreach (var prop in props)
                {
                    var expectedValue = prop.GetValue(expectedDot);
                    var actualValue = prop.GetValue(actualDot);
                    InnerEquals(expectedValue, actualValue, prop.PropertyType);
                }

                var fields = type.GetFields();
                foreach (var field in fields)
                {
                    var expectedValue = field.GetValue(expectedDot);
                    var actualValue = field.GetValue(actualDot);
                    InnerEquals(expectedValue, actualValue, field.FieldType);
                }

                return true;
            }

            private void InnerEquals(object expectedValue, object actualValue, Type type)
            {
                if (_notCheckTypes.Contains(type))
                {
                    return;
                }
                Assert.Equal(expectedValue, actualValue);
            }

            public int GetHashCode(T parameterValue)
            {
                return Tuple.Create(parameterValue).GetHashCode();
            }
        }
    }
}
