using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using FastTests;
using FastTests.Voron.Util;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Blittable
{
    public class SerializeAndDeserializeMergedTransactionCommandTests : RavenLowLevelTestBase
    {
        [Fact]
        public void SerializeAndDeserialize_MergedPutAttachmentCommand()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var database = CreateDocumentDatabase())
            {
                //Arrange
                var recordFilePath = NewDataPath();

                var attachmentStream = new StreamsTempFile(recordFilePath, database);
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
                var jsonSerializer = DocumentConventions.Default.CreateSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto();
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }

                var fromStream = SerializeTestHelper.SimulateSavingToFileAndLoading(context, blitCommand);

                //Deserialize
                AttachmentHandler.MergedPutAttachmentCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedPutAttachmentCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, 
                    new CustomComparer<AttachmentHandler.MergedPutAttachmentCommand>(new []{typeof(Stream)}));

                stream.Seek(0, SeekOrigin.Begin);
                var expectedStream = expected.Stream.ReadData();
                var actualStream = actual.Stream.ReadData();
                Assert.Equal(expectedStream, actualStream);
            }
        }

        [Fact]
        public void SerializeAndDeserialize_MergedPutCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var document = EntityToBlittable.ConvertCommandToBlittable(data, context);
                var changeVector = context.GetLazyString("Some Lazy String");
                var expected = new MergedPutCommand(document, "user/", changeVector, null);

                //Action
                var jsonSerializer = DocumentConventions.Default.CreateSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto();
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = SerializeTestHelper.SimulateSavingToFileAndLoading(context, blitCommand);
                MergedPutCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedPutCommand.MergedPutCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<MergedPutCommand>());
            }
        }

        [Fact]
        public void SerializeAndDeserialize_PatchDocumentCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var database = CreateDocumentDatabase())
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var arg = EntityToBlittable.ConvertCommandToBlittable(data, context);
                var patchRequest = new PatchRequest("", PatchRequestType.None);

                var expected = new PatchDocumentCommand(
                    context,
                    "Some Id",
                    context.GetLazyString("Some Lazy String"),
                    false,
                    (patchRequest, arg),
                    (null, null),
                    database,
                    false, false, false);

                //Action
                var jsonSerializer = DocumentConventions.Default.CreateSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto();
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = SerializeTestHelper.SimulateSavingToFileAndLoading(context, blitCommand);
                PatchDocumentCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(fromStream);

                    var dto = jsonSerializer.Deserialize<PatchDocumentCommandDto>(reader);
                    actual = dto.ToCommand(context, database);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<PatchDocumentCommand>());
            }
        }

        [Fact]
        public void SerializeAndDeserialize_DeleteDocumentCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                //Arrange
                var expected = new DeleteDocumentCommand("Some Id", "Some Change Vector", null);

                //Action
                var jsonSerializer = DocumentConventions.Default.CreateSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto();
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = SerializeTestHelper.SimulateSavingToFileAndLoading(context, blitCommand);
                DeleteDocumentCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(fromStream);

                    var dto = jsonSerializer.Deserialize<DeleteDocumentCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<DeleteDocumentCommand>());
            }
        }

        [Fact]
        public void SerializeAndDeserialize_MergedBatchCommandCommandTest()
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
                        Document = EntityToBlittable.ConvertCommandToBlittable(data, context),
                        Patch = new PatchRequest("Some Script", PatchRequestType.None)
                    }
                };
                var expected = new BatchHandler.MergedBatchCommand
                {
                    ParsedCommands = commands
                };

                //Action
                var jsonSerializer = DocumentConventions.Default.CreateSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto();
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = SerializeTestHelper.SimulateSavingToFileAndLoading(context, blitCommand);
                BatchHandler.MergedBatchCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedBatchCommandDto>(reader);
                    actual = dto.ToCommand(null, null);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<BatchHandler.MergedBatchCommand>());
            }
        }

        [Fact]
        public void SerializeAndDeserialize_MergedBatchPutCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var database = CreateDocumentDatabase())
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var expected = new DatabaseDestination.MergedBatchPutCommand(database, BuildVersionType.V4, null);
                var document = new DocumentItem
                {
                    Document = new Document
                    {
                        ChangeVector = "Some Change Vector",
                        Data = EntityToBlittable.ConvertCommandToBlittable(data, context),
                        Id = context.GetLazyString("Some Id")
                    }
                };
                expected.Add(document);

                //Action
                var jsonSerializer = DocumentConventions.Default.CreateSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto();
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = SerializeTestHelper.SimulateSavingToFileAndLoading(context, blitCommand);
                DatabaseDestination.MergedBatchPutCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedBatchPutCommandDto>(reader);
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
        public void SerializeAndDeserialize_MergedInsertBulkCommandTest()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var database = CreateDocumentDatabase())
            {
                //Arrange
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var commands = new[]
                {
                    new BatchRequestParser.CommandData
                    {
                        Id = "Some Id",
                        ChangeVector = context.GetLazyString("Some Lazy String"),
                        Document = EntityToBlittable.ConvertCommandToBlittable(data, context),
                        Patch = new PatchRequest("Some Script", PatchRequestType.None)
                    }
                };
                var expected = new BulkInsertHandler.MergedInsertBulkCommand
                {
                    NumberOfCommands = commands.Length,
                    Commands = commands
                };

                //Action
                var jsonSerializer = DocumentConventions.Default.CreateSerializer();
                BlittableJsonReaderObject blitCommand;
                using (var writer = new BlittableJsonWriter(context))
                {
                    var dto = expected.ToDto();
                    jsonSerializer.Serialize(writer, dto);
                    writer.FinalizeDocument();

                    blitCommand = writer.CreateReader();
                }
                var fromStream = SerializeTestHelper.SimulateSavingToFileAndLoading(context, blitCommand);
                BulkInsertHandler.MergedInsertBulkCommand actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Init(fromStream);

                    var dto = jsonSerializer.Deserialize<MergedInsertBulkCommandDto>(reader);
                    actual = dto.ToCommand(context, database);
                }

                //Assert
                Assert.Equal(expected, actual, new CustomComparer<BulkInsertHandler.MergedInsertBulkCommand>());
            }
        }

        private class CustomComparer<T> : IEqualityComparer<T> where T : TransactionOperationsMerger.IRecordableCommand
        {
            private static IEnumerable<Type> _notCheckTypes;

            public CustomComparer(): this(Array.Empty<Type>()) { }

            public CustomComparer(IEnumerable<Type> notCheckTypes)
            {
                _notCheckTypes = notCheckTypes;
            }

            public bool Equals(T expected, T actual)
            {
                var expectedDot = expected.ToDto();
                var actualDot = actual.ToDto();

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

            private static void InnerEquals(object expectedValue, object actualValue, Type type)
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
