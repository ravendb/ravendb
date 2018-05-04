using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Handlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public class ClusterTransactionCommand : CommandBase
    {
        public string Database;

        public class CompareExchangeCommand : IDynamicJson
        {
            public string Key;
            public long Index;
            public BlittableJsonReaderObject Item;
            public CommandType Type;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Key)] = Key,
                    [nameof(Index)] = Index,
                    [nameof(Item)] = Item,
                    [nameof(Type)] = Type.ToString()
                };
            }
        }

        public class DocumentCommand : IDynamicJson
        {
            public CommandType Type;
            public string Id;
            public BlittableJsonReaderObject Document;
            public string ChangeVector;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Type)] = Type.ToString(),
                    [nameof(Id)] = Id,
                    [nameof(Document)] = Document,
                    [nameof(ChangeVector)] = ChangeVector
                };
            }
        }

        public class ClusterTransactionOptions : IDynamicJson
        {
            public TimeSpan? WaitForIndexesTimeout;
            public bool WaitForIndexThrow;
            public List<string> SpecifiedIndexesQueryString;

            public TimeSpan? WaitForReplicasTimeout;
            public string NumberOfReplicas;
            public bool ThrowOnTimeoutInWaitForReplicas;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(WaitForIndexesTimeout)] = WaitForIndexesTimeout,
                    [nameof(WaitForIndexThrow)] = WaitForIndexThrow,
                    [nameof(SpecifiedIndexesQueryString)] = new DynamicJsonArray(SpecifiedIndexesQueryString),

                    [nameof(WaitForReplicasTimeout)] = WaitForReplicasTimeout,
                    [nameof(NumberOfReplicas)] = NumberOfReplicas,
                    [nameof(ThrowOnTimeoutInWaitForReplicas)] = ThrowOnTimeoutInWaitForReplicas,
                };
            }
        }

        public List<CompareExchangeCommand> ClusterCommands = new List<CompareExchangeCommand>();
        public BlittableJsonReaderObject SerializedDatabaseCommands;

        [JsonDeserializationIgnore]
        public ClusterTransactionOptions Options;

        [JsonDeserializationIgnore]
        public readonly List<DocumentCommand> DatabaseCommands = new List<DocumentCommand>();

        public ClusterTransactionCommand()
        {
        }

        public ClusterTransactionCommand(string database, ArraySegment<BatchRequestParser.CommandData> commandParsedCommands, ClusterTransactionOptions options)
        {
            Database = database;
            Options = options;

            foreach (var command in commandParsedCommands)
            {
                if (command.Type == CommandType.PUT || command.Type == CommandType.DELETE)
                {
                    DatabaseCommands.Add(new DocumentCommand
                    {
                        Type = command.Type,
                        ChangeVector = command.ChangeVector,
                        Document = command.Document,
                        Id = command.Id
                    });
                    continue;
                }
                if (command.Type == CommandType.CompareExchangePUT || command.Type == CommandType.CompareExchangeDELETE)
                {
                    ClusterCommands.Add(new CompareExchangeCommand
                    {
                        Key = command.Id,
                        Type = command.Type,
                        Index = long.Parse(command.ChangeVector),
                        Item = command.Document
                    });
                    continue;
                }
                throw new ArgumentException($"The type '{command.Type}' is not supported in '{nameof(ClusterTransactionCommand)}.'");
            }
        }

        public bool ExecuteCompareExchangeCommands(TransactionOperationContext context, long index, Table items)
        {
            if (ClusterCommands == null || ClusterCommands.Count == 0)
                return true;

            var toExecute = new List<CompareExchangeCommandBase>(ClusterCommands.Count);
            foreach (var clusterCommand in ClusterCommands)
            {
                if (clusterCommand.Type == CommandType.CompareExchangePUT)
                {
                    var put = new AddOrUpdateCompareExchangeCommand(clusterCommand.Key, clusterCommand.Item, clusterCommand.Index, context);
                    if (put.Validate(context, items, index) == false)
                    {
                        return false;
                    }
                    toExecute.Add(put);
                }

                if (clusterCommand.Type == CommandType.CompareExchangeDELETE)
                {
                    var delete = new RemoveCompareExchangeCommand(clusterCommand.Key, clusterCommand.Index, context);
                    if (delete.Validate(context, items, index) == false)
                    {
                        return false;
                    }
                    toExecute.Add(delete);
                }
            }

            foreach (var command in toExecute)
            {
                command.Execute(context, items, index);
            }

            return true;
        }

        public unsafe void SaveCommandsBatch(TransactionOperationContext context, long index)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);
            var commands = SerializedDatabaseCommands.Clone(context);
            using (Slice.From(context.Allocator, Database + "/", out Slice databaseSlice))
            using (items.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(databaseSlice.Content.Ptr, databaseSlice.Size);
                tvb.Add(Bits.SwapBytes(index));
                tvb.Add(commands.BasePointer, commands.Size);
                items.Insert(tvb);
            }
        }

        public enum TransactionCommandsColumn
        {
            Database,
            RaftIndex,
            Commands,
        }

        public static unsafe long ReadFirstIndex(TransactionOperationContext serverContext, string database)
        {
            var items = serverContext.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);

            using (Slice.From(serverContext.Allocator, database + "/", out var prefixSlice))
            {
                var reader = items.SeekOneForwardFrom(ClusterStateMachine.TransactionCommandsSchema.Indexes[ClusterStateMachine.CommandByDatabaseAndIndex], prefixSlice);
                if (reader == null)
                    return 0;
                var value = *(long*)reader.Reader.Read((int)TransactionCommandsColumn.RaftIndex, out var _);
                return Bits.SwapBytes(value);
            }
        }

        public static (BlittableJsonReaderArray Commands, ClusterTransactionOptions Options) ReadCommandsBatch(TransactionOperationContext context, string database, long index)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);

            var databaseBytes = Encoding.UTF8.GetBytes(database + "/");
            var indexBytes = BitConverter.GetBytes(Bits.SwapBytes(index));
            
            var prefix = new byte[databaseBytes.Length + sizeof(long)];
            databaseBytes.CopyTo(prefix, 0);
            indexBytes.CopyTo(prefix, databaseBytes.Length);

            using (Slice.From(context.Allocator, prefix, out Slice prefixSlice))
            {
                var commandsBluk = items.SeekForwardFrom(ClusterStateMachine.TransactionCommandsSchema.Indexes[ClusterStateMachine.CommandByDatabaseAndIndex],
                    prefixSlice, 0).Single();

                var reader = commandsBluk.Result.Reader;
                return ReadCommands(context, reader);
            }
        }

        private static unsafe (BlittableJsonReaderArray Commands, ClusterTransactionOptions Options) ReadCommands(TransactionOperationContext context, TableValueReader reader)
        {
            var ptr = reader.Read((int)TransactionCommandsColumn.Commands, out var size);
            if (ptr == null)
                return (null, null);

            var blittable = new BlittableJsonReaderObject(ptr, size, context);
            blittable.TryGet(nameof(DatabaseCommands), out BlittableJsonReaderArray array);
            ClusterTransactionOptions options = null;
            if (blittable.TryGet(nameof(Options), out BlittableJsonReaderObject blittableOptions))
            {
                options = JsonDeserializationServer.ClusterTransactionOptions(blittableOptions);
            }
            return (array, options);
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(ClusterCommands)] = new DynamicJsonArray(ClusterCommands.Select(x => x.ToJson()));
            if (SerializedDatabaseCommands == null && DatabaseCommands.Count > 0)
            {
                var databaseCommands = new DynamicJsonValue
                {
                    [nameof(DatabaseCommands)] = new DynamicJsonArray(DatabaseCommands.Select(x => x.ToJson())),
                    [nameof(Options)] = Options.ToJson(),
                };
                SerializedDatabaseCommands = context.ReadObject(databaseCommands, "read database commands");
            }
            djv[nameof(SerializedDatabaseCommands)] = SerializedDatabaseCommands;
            djv[nameof(Database)] = Database;

            return djv;
        }
    }
}
