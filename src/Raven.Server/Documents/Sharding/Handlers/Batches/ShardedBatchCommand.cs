using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public class ShardedBatchCommand : IBatchCommand
{
    private readonly TransactionOperationContext _context;
    private readonly ShardedDatabaseContext _databaseContext;

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

    public IEnumerator<SingleShardedCommand> GetCommands(ShardedBatchBehavior behavior)
    {
        int? previousBucket = null;
        string previousDocumentId = null;

        var streamPosition = 0;
        var positionInResponse = 0;
        for (var index = 0; index < ParsedCommands.Count; index++)
        {
            var cmd = ParsedCommands[index];
            var bufferedCommand = BufferedCommands[index];

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

                    var result = _databaseContext.GetShardNumberAndBucketFor(_context, id);
                    if (idsByShard.TryGetValue(result.ShardNumber, out var list) == false)
                        idsByShard[result.ShardNumber] = list = new List<(string Id, string ChangeVector)>();

                    list.Add((id, expectedChangeVector));

                    AssertBehavior(behavior, CommandType.BatchPATCH, ref previousDocumentId, ref previousBucket, result.Bucket, id);
                }

                foreach (var kvp in idsByShard)
                {
                    yield return new SingleShardedCommand
                    {
                        ShardNumber = kvp.Key,
                        CommandStream = bufferedCommand.ModifyBatchPatchStream(kvp.Value),
                        PositionInResponse = positionInResponse
                    };
                }

                positionInResponse++;
                continue;
            }

            var commandStream = bufferedCommand.CommandStream;
            if (bufferedCommand.ModifyIdentityStreamRequired)
            {
                ModifyId(ref cmd, bufferedCommand.IsServerSideIdentity);
                commandStream = bufferedCommand.ModifyIdentityStream(cmd);
            }

            var cmdResult = GetShardNumberForCommandType(cmd, bufferedCommand.IsServerSideIdentity, out var documentId);
            var stream = cmd.Type == CommandType.AttachmentPUT ? AttachmentStreams[streamPosition++] : null;

            AssertBehavior(behavior, cmd.Type, ref previousDocumentId, ref previousBucket, cmdResult.Bucket, documentId);

            yield return new SingleShardedCommand
            {
                ShardNumber = cmdResult.ShardNumber,
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
