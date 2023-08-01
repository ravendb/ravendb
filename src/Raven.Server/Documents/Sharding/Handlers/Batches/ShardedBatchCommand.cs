using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public sealed class ShardedBatchCommand : IBatchCommand
{
    private readonly TransactionOperationContext _context;
    private readonly ShardedDatabaseContext _databaseContext;
    
    private Dictionary<int, ShardedSingleNodeBatchCommand> _batchPerShard;
    private object[] _result;
    public DynamicJsonArray Result => new DynamicJsonArray(_result);

    public List<BufferedCommand> BufferedCommands;
    public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;
    public List<Stream> AttachmentStreams;

    public HashSet<string> ModifiedCollections { get; set; }

    public string LastChangeVector { get; set; }

    public long LastTombstoneEtag { get; set; }

    public bool IsClusterTransaction { get; set; }
    
    internal ShardedBatchCommand(TransactionOperationContext context, ShardedDatabaseContext databaseContext)
    {
        _context = context;
        _databaseContext = databaseContext;
    }

    public Dictionary<int, ShardedSingleNodeBatchCommand> GetCommands(ShardedBatchBehavior behavior, IndexBatchOptions indexBatchOptions, ReplicationBatchOptions replicationBatchOptions)
    {
        var commands = _batchPerShard == null ? GetNewCommands(behavior) : GetRetryCommands();

        var resultSize = 0;
        _batchPerShard ??= new Dictionary<int, ShardedSingleNodeBatchCommand>();
        foreach (var c in commands)
        {
            var shardNumber = c.ShardNumber;
            if (_batchPerShard.TryGetValue(shardNumber, out var shardedBatchCommand) == false)
            {
                shardedBatchCommand = new ShardedSingleNodeBatchCommand(_databaseContext.ShardExecutor.Conventions, shardNumber, indexBatchOptions, replicationBatchOptions);
                _batchPerShard.Add(shardNumber, shardedBatchCommand);
            }

            shardedBatchCommand.AddCommand(c);
            resultSize++;
        }

        _result ??= new object[resultSize];
        return _batchPerShard;
    }

    public void MarkShardAsComplete(JsonOperationContext context, int shardNumber, bool isFromStudio)
    {
        _batchPerShard.Remove(shardNumber, out var command);
        command!.AssembleShardedReply(context, _result, isFromStudio ? shardNumber : null);
        command.Dispose();
    }

    private IEnumerator<SingleShardedCommand> GetRetryCommands()
    {
        var failed = _batchPerShard.Values.ToList();
        _batchPerShard.Clear();
        foreach (var failedShard in failed)
        {
            foreach (var command in failedShard.Commands)
            {
                foreach (var retry in command.Retry(_databaseContext, _context))
                {
                    yield return retry;
                }
            }
        }
    }

    private IEnumerator<SingleShardedCommand> GetNewCommands(ShardedBatchBehavior behavior)
    {
        int? previousBucket = null;
        string previousDocumentId = null;

        var streamPosition = 0;
        var positionInResponse = 0;
        for (var index = 0; index < ParsedCommands.Count; index++)
        {
            var cmd = ParsedCommands[index];
            var bufferedCommand = BufferedCommands[index];

            (int ShardNumber, int Bucket) result;
            int shardNumber;
            if (cmd.Type == CommandType.BatchPATCH)
            {
                var idsByShard = new Dictionary<int, List<(string Id, string ChangeVector)>>();
                foreach (var cmdId in cmd.Ids)
                {
                    if (!(cmdId is BlittableJsonReaderObject bjro))
                        throw new InvalidOperationException();

                    if (bjro.TryGet(nameof(ICommandData.Id), out string id) == false)
                        throw new InvalidOperationException();

                    bjro.TryGet(nameof(ICommandData.ChangeVector), out string expectedChangeVector);

                    result = _databaseContext.GetShardNumberAndBucketFor(_context, id);
                    if (idsByShard.TryGetValue(result.ShardNumber, out var list) == false)
                        idsByShard[result.ShardNumber] = list = new List<(string Id, string ChangeVector)>();

                    list.Add((id, expectedChangeVector));

                    AssertBehavior(behavior, CommandType.BatchPATCH, ref previousDocumentId, ref previousBucket, result.Bucket, id);
                }

                foreach (var kvp in idsByShard)
                {
                    var list = kvp.Value;
                    shardNumber = _databaseContext.ForTestingPurposes?.ModifyShardNumber(kvp.Key) ?? kvp.Key;

                    yield return new BatchPatchSingleShardedCommand
                    {
                        ShardNumber = shardNumber,
                        BufferedCommand = bufferedCommand,
                        List = list,
                        CommandStream = bufferedCommand.ModifyBatchPatchStream(list),
                        PositionInResponse = positionInResponse++
                    };
                }

                continue;
            }

            var commandStream = bufferedCommand.CommandStream;
            if (cmd.Type == CommandType.DELETE && cmd.IdPrefixed)
            {
                if (behavior == ShardedBatchBehavior.TransactionalSingleBucketOnly)
                    throw new ShardedBatchBehaviorViolationException($"Batch command '{nameof(DeletePrefixedCommandData)}' (prefixed id : '{cmd.Id}') does not operate on a single bucket as this is a multi-shard operation," +
                                                                     "which violates the requested sharded batch behavior to operate on a single bucket only.");
                // send delete-prefixed-command to all shards
                var keys = _databaseContext.DatabaseRecord.Sharding.Shards.Keys;
                foreach (var key in keys)
                {
                    shardNumber = _databaseContext.ForTestingPurposes?.ModifyShardNumber(key) ?? key;
                    yield return new SingleShardedCommand
                    {
                        Id = cmd.Id,
                        ShardNumber = shardNumber,
                        CommandStream = commandStream,
                        PositionInResponse = positionInResponse++
                    };
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal,
                    "if we have prefixed sharding configured we might not need to send the command to all of the shards");

                continue;
            }

            if (bufferedCommand.ModifyIdentityStreamRequired)
            {
                ModifyId(ref cmd, bufferedCommand.IsServerSideIdentity);
                commandStream = bufferedCommand.ModifyIdentityStream(cmd);
            }

            result = GetShardNumberForCommandType(cmd, bufferedCommand.IsServerSideIdentity, out var documentId);
            var stream = cmd.Type == CommandType.AttachmentPUT ? AttachmentStreams[streamPosition++] : null;

            AssertBehavior(behavior, cmd.Type, ref previousDocumentId, ref previousBucket, result.Bucket, documentId);

            shardNumber = _databaseContext.ForTestingPurposes?.ModifyShardNumber(result.ShardNumber) ?? result.ShardNumber;

            yield return new SingleShardedCommand
            {
                Id = cmd.Id,
                ShardNumber = shardNumber,
                AttachmentStream = stream,
                CommandStream = commandStream,
                PositionInResponse = positionInResponse++
            };
        }

        static void AssertBehavior(ShardedBatchBehavior behavior, CommandType commandType, ref string previousDocumentId, ref int? previousBucket, int bucket, string documentId)
        {
            if (previousBucket == null)
            {
                previousBucket = bucket;
                previousDocumentId = documentId;
                return;
            }

            switch (behavior)
            {
                case ShardedBatchBehavior.TransactionalSingleBucketOnly:
                    if (previousBucket != bucket)
                        throw new ShardedBatchBehaviorViolationException($"Batch command of type '{commandType}' operates on a document ID '{documentId}' with shard bucket '{bucket}' which violates the requested sharded batch behavior to operate on a single bucket ('{previousBucket}' from document ID '{previousDocumentId}'). Consider using cluster transactions in order to modify documents from multiple buckets without violating transaction boundaries.");
                    break;
                case ShardedBatchBehavior.NonTransactionalMultiBucket:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(behavior), behavior, null);
            }
        }
    }

    private void ModifyId(ref BatchRequestParser.CommandData cmd, bool isServerSideIdentity)
    {
        if (isServerSideIdentity)
            cmd.Id = ShardHelper.GenerateStickyId(cmd.Id, _databaseContext.IdentityPartsSeparator); // generated id is 'users/$BASE26$/'

        if (string.IsNullOrEmpty(cmd.Id))
            cmd.Id = Guid.NewGuid().ToString();
    }

    private (int ShardNumber, int Bucket) GetShardNumberForCommandType(BatchRequestParser.CommandData cmd, bool isServerSideIdentity, out string documentId)
    {
        documentId = cmd.Id;
        if (isServerSideIdentity)
            return _databaseContext.GetShardNumberAndBucketForIdentity(_context, documentId);

        if (cmd.Type == CommandType.Counters)
            documentId ??= cmd.Counters.DocumentId;

        return _databaseContext.GetShardNumberAndBucketFor(_context, documentId);
    }

    public void Dispose()
    {
    }
}
