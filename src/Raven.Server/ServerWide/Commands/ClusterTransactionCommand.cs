using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public class ClusterTransactionCommand : CommandBase
    {
        public string Database;
        public long DatabaseCommandsCount;

        public class ClusterTransactionDataCommand
        {
            public CommandType Type;
            public string Id;
            public BlittableJsonReaderObject Document;
            public string ChangeVector;
            public long Index;

            public static ClusterTransactionDataCommand FromCommandData(BatchRequestParser.CommandData command)
            {
                if (command.IdPrefixed)
                {
                    throw new NotSupportedException("Deleting by prefix is not supported on cluster transaction.");
                }

                return new ClusterTransactionDataCommand
                {
                    Id = command.Id,
                    ChangeVector = command.ChangeVector,
                    Document = command.Document,
                    Index = command.Index,
                    Type = command.Type
                };
            }

            public DynamicJsonValue ToJson(JsonOperationContext context)
            {
                return new DynamicJsonValue
                {
                    [nameof(Type)] = Type,
                    [nameof(Id)] = Id,
                    [nameof(Index)] = Index,
                    [nameof(ChangeVector)] = ChangeVector,
                    [nameof(Document)] = Document?.Clone(context)
                };
            }
        }

        public class ClusterTransactionOptions : IDynamicJson
        {
            public string TaskId;
            public TimeSpan? WaitForIndexesTimeout;
            public bool WaitForIndexThrow;
            public List<string> SpecifiedIndexesQueryString;

            public ClusterTransactionOptions() { }

            public ClusterTransactionOptions(string taskId)
            {
                TaskId = taskId;
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(TaskId)] = TaskId,
                    [nameof(WaitForIndexesTimeout)] = WaitForIndexesTimeout,
                    [nameof(WaitForIndexThrow)] = WaitForIndexThrow,
                    [nameof(SpecifiedIndexesQueryString)] = SpecifiedIndexesQueryString != null ? new DynamicJsonArray(SpecifiedIndexesQueryString) : null,
                };
            }
        }

        public List<ClusterTransactionDataCommand> ClusterCommands = new List<ClusterTransactionDataCommand>();
        public BlittableJsonReaderObject SerializedDatabaseCommands;

        [JsonDeserializationIgnore]
        public ClusterTransactionOptions Options;

        [JsonDeserializationIgnore]
        public readonly List<ClusterTransactionDataCommand> DatabaseCommands = new List<ClusterTransactionDataCommand>();

        public static Slice CommandsCountKey;

        static ClusterTransactionCommand()
        {
            Slice.From(StorageEnvironment.LabelsContext, "CommandsCountKey", out CommandsCountKey);
        }

        public ClusterTransactionCommand() { }

        public ClusterTransactionCommand(string database, ArraySegment<BatchRequestParser.CommandData> commandParsedCommands, ClusterTransactionOptions options)
        {
            Database = database;
            Options = options;

            foreach (var commandData in commandParsedCommands)
            {
                var command = ClusterTransactionDataCommand.FromCommandData(commandData);
                switch (commandData.Type)
                {
                    case CommandType.PUT:
                    case CommandType.DELETE:
                        DatabaseCommands.Add(command);
                        break;
                    case CommandType.CompareExchangePUT:
                    case CommandType.CompareExchangeDELETE:
                        ClusterCommands.Add(command);
                        break;
                    default:
                        throw new ArgumentException($"The type '{commandData.Type}' is not supported in '{nameof(ClusterTransactionCommand)}.'");
                }
            }

            DatabaseCommandsCount = DatabaseCommands.Count;
        }

        public List<string> ExecuteCompareExchangeCommands(TransactionOperationContext context, long index, Table items)
        {
            if (ClusterCommands == null || ClusterCommands.Count == 0)
                return null;

            var toExecute = new List<CompareExchangeCommandBase>(ClusterCommands.Count);
            var errors = new List<string>();
            foreach (var clusterCommand in ClusterCommands)
            {
                long current;
                switch (clusterCommand.Type)
                {
                    case CommandType.CompareExchangePUT:
                        var put = new AddOrUpdateCompareExchangeCommand(Database, clusterCommand.Id, clusterCommand.Document, clusterCommand.Index, context);
                        if (put.Validate(context, items, clusterCommand.Index, out current) == false)
                        {
                            errors.Add(
                                $"Concurrency check failed for putting the key '{clusterCommand.Id}'. Requested index: {clusterCommand.Index}, actual index: {current}");
                            continue;
                        }
                        toExecute.Add(put);
                        break;
                    case CommandType.CompareExchangeDELETE:
                        var delete = new RemoveCompareExchangeCommand(Database, clusterCommand.Id, clusterCommand.Index, context);
                        if (delete.Validate(context, items, clusterCommand.Index, out current) == false)
                        {
                            errors.Add($"Concurrency check failed for deleting the key '{clusterCommand.Id}'. Requested index: {clusterCommand.Index}, actual index: {current}");
                            continue;
                        }
                        toExecute.Add(delete);
                        break;
                    default:
                        throw new InvalidOperationException("Invalid cluster command detected: " + clusterCommand.Type + "! Only CompareExchangePUT and CompareExchangeDELETE are supported.");
                }
            }

            if (errors.Count > 0)
            {
                return errors;
            }

            foreach (var command in toExecute)
            {
                command.Execute(context, items, index);
            }

            return null;
        }
        
        public unsafe void SaveCommandsBatch(TransactionOperationContext context, long index)
        {
            if (SerializedDatabaseCommands == null || DatabaseCommandsCount == 0)
                return;

            var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);
            var commandsIndexTree = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.TransactionCommandsIndex);
            var commands = SerializedDatabaseCommands.Clone(context);

            using (Slice.From(context.Allocator, Database.ToLowerInvariant(), out Slice databaseSlice))
            using (items.Allocate(out TableValueBuilder tvb))
            {
                var count = commandsIndexTree.ReadLong(CommandsCountKey) ?? 0;

                tvb.Add(databaseSlice.Content.Ptr, databaseSlice.Size);
                tvb.Add(Separator);
                tvb.Add(Bits.SwapBytes(index));
                tvb.Add(commands.BasePointer, commands.Size);
                tvb.Add(count);
                items.Insert(tvb);

                using (commandsIndexTree.DirectAdd(CommandsCountKey, sizeof(long), out byte* ptr))
                    *(long*)ptr = count + DatabaseCommandsCount;
            }
        }

        public enum TransactionCommandsColumn
        {
            Database,
            Saparator,
            RaftIndex,
            Commands,
            PreviousCount
        }

        public static unsafe long ReadFirstIndex(TransactionOperationContext serverContext, string database)
        {
            var items = serverContext.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);
            using (GetPrefix(serverContext, database, out var prefixSlice))
            {
                var indexDef = ClusterStateMachine.TransactionCommandsSchema.Indexes[ClusterStateMachine.CommandByDatabaseAndIndex];
                var reader = items.SeekOneForwardFromPrefix(indexDef, prefixSlice);
                if (reader == null)
                    return 0;
                var value = *(long*)reader.Reader.Read((int)TransactionCommandsColumn.RaftIndex, out var _);
                return Bits.SwapBytes(value);
            }
        }

        public const byte Separator = 30;

        public static unsafe ByteStringContext.InternalScope GetPrefix(TransactionOperationContext contenxt, string database, out Slice prefixSlice, long? index = null)
        {
            var maxSize = database.GetUtf8MaxSize() + sizeof(byte);
            if (index.HasValue)
                maxSize += sizeof(long);

            var lowerBufferSize = database.Length * sizeof(char);


            var scope = contenxt.Allocator.Allocate(maxSize + lowerBufferSize, out var prefixBuffer);
            try
            {

                fixed (char* pDb = database)
                {
                    var lowerBufferStart = (char*)(prefixBuffer.Ptr + maxSize);
                    for (int i = 0; i < database.Length; i++)
                    {
                        lowerBufferStart[i] = char.ToLowerInvariant(pDb[i]);
                    }

                    var dbLen = Encoding.UTF8.GetBytes(lowerBufferStart, database.Length, prefixBuffer.Ptr, prefixBuffer.Length);
                    prefixBuffer.Ptr[dbLen] = Separator;
                    var actualSize = dbLen + 1;
                    if (index.HasValue)
                    {
                        *(long*)(prefixBuffer.Ptr + actualSize) = Bits.SwapBytes(index.Value);
                        actualSize += sizeof(long);
                    }
                    prefixBuffer.Truncate(actualSize);
                    prefixSlice = new Slice(prefixBuffer);
                    return scope;
                }
            }
            catch
            {
                scope.Dispose();
                throw;
            }
        }

        public class SingleClusterDatabaseCommand
        {
            public ClusterTransactionOptions Options;
            public BlittableJsonReaderArray Commands;
            public long Index;
            public long PreviousCount;
            public string Database;
        }

        public static IEnumerable<SingleClusterDatabaseCommand> ReadCommandsBatch(TransactionOperationContext context, string database, long fromIndex)
        {
            var lowerDb = database.ToLowerInvariant();
            var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);
            using (GetPrefix(context, database, out Slice prefixSlice, fromIndex))
            {
                var schemaIndexDef = ClusterStateMachine.TransactionCommandsSchema.Indexes[ClusterStateMachine.CommandByDatabaseAndIndex];
                var commandsBulk = items.SeekForwardFrom(schemaIndexDef, prefixSlice, 0);

                foreach (var command in commandsBulk)
                {
                    var reader = command.Result.Reader;
                    var result = ReadCommand(context, reader);
                    if (result == null)
                        yield break;
                    if (result.Database != lowerDb) // beware of reading commands of other databases.
                        yield break;
                    if (result.Index < fromIndex)
                        continue;
                    yield return result;
                }
            }
        }

        private static unsafe SingleClusterDatabaseCommand ReadCommand(TransactionOperationContext context, TableValueReader reader)
        {
            var ptr = reader.Read((int)TransactionCommandsColumn.Commands, out var size);
            if (ptr == null)
                return null;
            var blittable = new BlittableJsonReaderObject(ptr, size, context);
            blittable.TryGet(nameof(DatabaseCommands), out BlittableJsonReaderArray array);

            ClusterTransactionOptions options = null;
            if (blittable.TryGet(nameof(Options), out BlittableJsonReaderObject blittableOptions))
            {
                options = JsonDeserializationServer.ClusterTransactionOptions(blittableOptions);
            }

            var index = *(long*)reader.Read((int)TransactionCommandsColumn.RaftIndex, out _);
            var count = *(long*)reader.Read((int)TransactionCommandsColumn.PreviousCount, out _);

            var databasePtr = reader.Read((int)TransactionCommandsColumn.Database, out size);
            var databse = new LazyStringValue(null, databasePtr, size, context);
            return new SingleClusterDatabaseCommand
            {
                Options = options,
                Commands = array,
                Index = Bits.SwapBytes(index),
                PreviousCount = count,
                Database = databse
            };
        }

        public override object FromRemote(object remoteResult)
        {
            var rc = new List<string>();
            if (remoteResult is BlittableJsonReaderArray array)
            {
                foreach (var o in array)
                {
                    rc.Add(o.ToString());
                }

                return rc;
            }
            return base.FromRemote(remoteResult);
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(ClusterCommands)] = new DynamicJsonArray(ClusterCommands.Select(x => x.ToJson(context)));
            if (SerializedDatabaseCommands == null && DatabaseCommands.Count > 0)
            {
                var databaseCommands = new DynamicJsonValue
                {
                    [nameof(DatabaseCommands)] = new DynamicJsonArray(DatabaseCommands.Select(x => x.ToJson(context))),
                    [nameof(Options)] = Options.ToJson(),
                };
                SerializedDatabaseCommands = context.ReadObject(databaseCommands, "read database commands");
            }
            djv[nameof(SerializedDatabaseCommands)] = SerializedDatabaseCommands;
            djv[nameof(Database)] = Database;
            djv[nameof(DatabaseCommandsCount)] = DatabaseCommandsCount;

            return djv;
        }
    }
}
