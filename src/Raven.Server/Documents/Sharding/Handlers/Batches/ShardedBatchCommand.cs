using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
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

                    AssertBehavior(behavior, CommandType.BatchPATCH, ref previousBucket, result.Bucket);
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

            var cmdResult = GetShardNumberForCommandType(cmd, bufferedCommand.IsServerSideIdentity);
            var stream = cmd.Type == CommandType.AttachmentPUT ? AttachmentStreams[streamPosition++] : null;

            AssertBehavior(behavior, cmd.Type, ref previousBucket, cmdResult.Bucket);

            yield return new SingleShardedCommand
            {
                ShardNumber = cmdResult.ShardNumber,
                AttachmentStream = stream,
                CommandStream = commandStream,
                PositionInResponse = positionInResponse++
            };
        }

        static void AssertBehavior(ShardedBatchBehavior behavior, CommandType commandType, ref int? previousBucket, int bucket)
        {
            if (previousBucket == null)
            {
                previousBucket = bucket;
                return;
            }

            switch (behavior)
            {
                case ShardedBatchBehavior.SingleBucket:
                    if (previousBucket != bucket)
                        throw new InvalidOperationException($"Batch command of type '{commandType}' operates on a shard bucket '{bucket}' which violates the requested sharded batch behavior to operate on a single bucket ('{previousBucket}').");
                    break;
                case ShardedBatchBehavior.MultiBucket:
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

        if (string.Empty == cmd.Id)
            cmd.Id = Guid.NewGuid().ToString();
    }

    private (int ShardNumber, int Bucket) GetShardNumberForCommandType(BatchRequestParser.CommandData cmd, bool isServerSideIdentity)
    {
        if (isServerSideIdentity)
            return _databaseContext.GetShardNumberAndBucketForIdentity(_context, cmd.Id);

        if (cmd.Type == CommandType.Counters)
            return _databaseContext.GetShardNumberAndBucketFor(_context, cmd.Id ?? cmd.Counters.DocumentId);

        return _databaseContext.GetShardNumberAndBucketFor(_context, cmd.Id);
    }

    public void Dispose()
    {
    }
}
