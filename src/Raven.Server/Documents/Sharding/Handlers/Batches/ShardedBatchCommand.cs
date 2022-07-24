using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public class ShardedBatchCommand : IBatchCommand, IEnumerable<SingleShardedCommand>
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

    public IEnumerator<SingleShardedCommand> GetEnumerator()
    {
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

                    var shardId = _databaseContext.GetShardNumber(_context, id);
                    if (idsByShard.TryGetValue(shardId, out var list) == false)
                    {
                        list = new List<(string Id, string ChangeVector)>();
                        idsByShard.Add(shardId, list);
                    }
                    list.Add((id, expectedChangeVector));
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

            int shardNumber;
            if (bufferedCommand.IsServerSideIdentity)
            {
                cmd.Id = ShardHelper.GenerateStickyId(cmd.Id, _databaseContext.IdentityPartsSeparator);
                shardNumber = _databaseContext.GetShardNumber(_context, cmd.Id.AsSpan(0, cmd.Id.Length - 2));
            }
            else
            {
                shardNumber = _databaseContext.GetShardNumber(_context, cmd.Id);
            }

            var stream = cmd.Type == CommandType.AttachmentPUT ? AttachmentStreams[streamPosition++] : null;
            var commandStream = bufferedCommand.CommandStream;
            if (bufferedCommand.IsIdentity || bufferedCommand.IsServerSideIdentity)
            {
                commandStream = bufferedCommand.ModifyIdentityStream(cmd.Id);
            }

            yield return new SingleShardedCommand
            {
                ShardNumber = shardNumber,
                AttachmentStream = stream,
                CommandStream = commandStream,
                PositionInResponse = positionInResponse++
            };
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Dispose()
    {
    }
}
