using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.Util;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Handlers.Processors.BulkInsert;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Blittable
{
    public class SerializeAndDeserializeMergedTransactionCommandTests : RavenLowLevelTestBase
    {
        public SerializeAndDeserializeMergedTransactionCommandTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task SerializeAndDeserialize_PutResolvedConflictsCommand()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
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
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, expectedDto);

                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                //Deserialize
                var actualDto = ReplayTxCommandHelper.Deserialize<PutResolvedConflictsCommandDto>(context, fromStream);

                //Assert
                //                    Assert.Equal(expectedDto.)
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task SerializeAndDeserialize_MergedDocumentReplicationCommand()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var item = new DocumentReplicationItem
                {
                    Type = ReplicationBatchItem.ReplicationItemType.Document,
                    Etag = 3,
                    ChangeVector = "A:1-abc",
                    Data = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new { Name = "Avi" }, context),
                    Id = context.GetLazyString("users/1"),
                    Collection = context.GetLazyString("Users"),
                    Context = context
                };

                var expectedDto = new MergedDocumentReplicationCommandDto
                {
                    LastEtag = 3,
                    SupportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Replication, TcpConnectionHeaderMessage.ReplicationTcpVersion),
                    ReplicatedItemDtos = [item.Clone(context, context.Allocator)],
                    SourceDatabaseId = "source",
                    ReplicatedAttachmentStreams = [KeyValuePair.Create("hash", (Stream)new MemoryStream([1, 2, 3]))]
                };

                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, expectedDto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);
                var actualDto = ReplayTxCommandHelper.Deserialize<MergedDocumentReplicationCommandDto>(context, fromStream);

                Assert.Equal(3, actualDto.LastEtag);
                Assert.Equal("source", actualDto.SourceDatabaseId);
                Assert.Equal(expectedDto.SupportedFeatures.Replication.MissingAttachments, actualDto.SupportedFeatures.Replication.MissingAttachments);
                Assert.Equal(expectedDto.SupportedFeatures.Replication.TimeSeries, actualDto.SupportedFeatures.Replication.TimeSeries);

                var actualItem = Assert.IsType<DocumentReplicationItem>(Assert.Single(actualDto.ReplicatedItemDtos));
                Assert.Equal(ReplicationBatchItem.ReplicationItemType.Document, actualItem.Type);
                Assert.Equal("A:1-abc", actualItem.ChangeVector);
                Assert.Equal("users/1", actualItem.Id.ToString());
                Assert.Equal("Users", actualItem.Collection.ToString());
                Assert.True(actualItem.Data.TryGet("Name", out string name));
                Assert.Equal("Avi", name);
                Assert.Null(actualItem.Context);

                var (hash, stream) = Assert.Single(actualDto.ReplicatedAttachmentStreams);
                Assert.Equal("hash", hash);
                Assert.Equal([1, 2, 3], ((MemoryStream)stream).ToArray());
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task SerializeAndDeserialize_MergedDeleteAttachmentCommand()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
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
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);

                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                //Deserialize
                AttachmentHandler.MergedDeleteAttachmentCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<MergedDeleteAttachmentCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(null, null);

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<AttachmentHandler.MergedDeleteAttachmentCommand, DocumentsOperationContext, DocumentsTransaction>(context));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task SerializeAndDeserialize_MergedPutAttachmentCommand()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
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
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);

                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                //Deserialize
                AttachmentHandler.MergedPutAttachmentCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<MergedPutAttachmentCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(null, null);

                //Assert
                Assert.Equal(expected, actual,
                    new CustomComparer<AttachmentHandler.MergedPutAttachmentCommand, DocumentsOperationContext, DocumentsTransaction>(context, new[] { typeof(Stream) }));

                stream.Seek(0, SeekOrigin.Begin);
                var expectedStream = await expected.Stream.ReadDataAsync();
                var actualStream = await actual.Stream.ReadDataAsync();
                Assert.Equal(expectedStream, actualStream);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task SerializeAndDeserialize_MergedPutCommandTest()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var document = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                var changeVector = context.GetLazyString("Some Lazy String");
                var expected = new MergedPutCommand(document, "user/", changeVector, null);

                //Action
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                MergedPutCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<MergedPutCommand.MergedPutCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(null, null);

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<MergedPutCommand, DocumentsOperationContext, DocumentsTransaction>(context));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
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
                    null,
                    database.IdentityPartsSeparator,
                    false, false, false, false);

                //Action
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);
                PatchDocumentCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<PatchDocumentCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(context, database);

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<PatchDocumentCommand, DocumentsOperationContext, DocumentsTransaction>(context));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task SerializeAndDeserialize_DeleteDocumentCommandTest()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                //Arrange
                var expected = new DeleteDocumentCommand("Some Id", "Some Change Vector", null);

                //Action
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                DeleteDocumentCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<DeleteDocumentCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(null, null);

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<DeleteDocumentCommand, DocumentsOperationContext, DocumentsTransaction>(context));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task SerializeAndDeserialize_MergedBatchCommandCommandTest()
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
                        Id = "Some Id",
                        ChangeVector = context.GetLazyString("Some Lazy String"),
                        Document = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context),
                        Patch = new PatchRequest("Some Script", PatchRequestType.None)
                    }
                };
                var expected = new MergedBatchCommand(null)
                {
                    ParsedCommands = commands
                };

                //Action
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                MergedBatchCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<MergedBatchCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(null, null);

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<MergedBatchCommand, DocumentsOperationContext, DocumentsTransaction>(context));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
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
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                DatabaseDestination.MergedBatchPutCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<DatabaseDestination.MergedBatchPutCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(context, database);

                //Assert
                var expectedDoc = expected.Documents[0].Document;
                var actualDoc = actual.Documents[0].Document;

                Assert.Equal(expectedDoc.Id, actualDoc.Id);
                Assert.Equal(expectedDoc.ChangeVector, actualDoc.ChangeVector);
                Assert.Equal(expectedDoc.Data, actualDoc.Data);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
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
                var expected = new MergedInsertBulkCommand
                {
                    NumberOfCommands = commands.Length,
                    Commands = commands
                };

                //Action
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                MergedInsertBulkCommand actual;
                var actualDto = ReplayTxCommandHelper.Deserialize<MergedInsertBulkCommandDto>(context, fromStream);
                actual = actualDto.ToCommand(context, database);

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<MergedInsertBulkCommand, DocumentsOperationContext, DocumentsTransaction>(context));
            }
        }

        [RavenFact(RavenTestCategory.TimeSeries)]
        public async Task SerializeAndDeserialize_TimeSeriesRetentionCommandTest()
        {
            using (var database = CreateDocumentDatabase())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                //Arrange
                Slice.From(context.Allocator, "users/1", out Slice key);
                var to = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
                var expected = new TimeSeriesRollups.TimeSeriesRetentionCommand(new List<Slice> { key }, "Users", to);

                //Action
                var dto = expected.ToDto(context);
                BlittableJsonReaderObject blitCommand = ReplayTxCommandHelper.Serialize(context, dto);
                var fromStream = await SerializeTestHelper.SimulateSavingToFileAndLoadingAsync(context, blitCommand);

                var actualDto = ReplayTxCommandHelper.Deserialize<TimeSeriesRollups.TimeSeriesRetentionCommand.TimeSeriesRetentionCommandDto>(context, fromStream);

                //Assert
                Assert.Equal("users/1", Assert.Single(actualDto._keys).ToString());
                Assert.Equal("Users", actualDto._collection);
                Assert.Equal(to, actualDto._to);
                Assert.NotNull(actualDto.ToCommand(context, database));
            }
        }

        private class CustomComparer<TRecordableCommand, TOperationContext, TTransaction> : IEqualityComparer<TRecordableCommand>
            where TOperationContext : TransactionOperationContext<TTransaction>
            where TRecordableCommand : IRecordableCommand<TOperationContext, TTransaction>
            where TTransaction : RavenTransaction
        {
            private readonly IEnumerable<Type> _notCheckTypes;
            private readonly TOperationContext _context;

            public CustomComparer(TOperationContext context) : this(context, Array.Empty<Type>())
            {
            }

            public CustomComparer(TOperationContext context, IEnumerable<Type> notCheckTypes)
            {
                _context = context;
                _notCheckTypes = notCheckTypes;
            }

            public bool Equals(TRecordableCommand expected, TRecordableCommand actual)
            {
                if (expected == null && actual == null)
                    return true;

                if (expected == null || actual == null)
                    return false;

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

                if (type == typeof(BatchRequestParser.CommandData[]))
                {
                    var a1 = (BatchRequestParser.CommandData[])expectedValue;
                    var a2 = (BatchRequestParser.CommandData[])actualValue;

                    Assert.Equal(a1.Length, a2.Length);
                    for (int i = 0; i < a1.Length; i++)
                    {
                        Assert.True(ObjectsAreEquals(a1[i], a2[i]));
                    }
                }
                else
                    Assert.Equal(expectedValue, actualValue);
            }

            public int GetHashCode(TRecordableCommand parameterValue)
            {
                return Tuple.Create(parameterValue).GetHashCode();
            }


            private static bool ObjectsAreEquals(object d1, object d2)
            {
                if ((d1 == null) || d1.GetType().Equals(d2.GetType()) == false)
                {
                    return false;
                }

                if (d1 != null && d2 != null)
                {
                    Type type = d1.GetType();
                    foreach (System.Reflection.PropertyInfo pi in type.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
                    {
                        object selfValue = type.GetProperty(pi.Name).GetValue(d1, null);
                        object toValue = type.GetProperty(pi.Name).GetValue(d2, null);

                        if (selfValue != toValue && (selfValue == null || !selfValue.Equals(toValue)))
                        {
                            return false;
                        }
                    }

                    return true;
                }
                return d1 == d2;
            }

        }
    }
}
