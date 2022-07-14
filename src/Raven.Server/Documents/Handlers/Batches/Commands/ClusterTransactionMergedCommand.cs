using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

public class ClusterTransactionMergedCommand : TransactionMergedCommand
{
    private readonly ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand> _batch;
    public readonly Dictionary<long, DynamicJsonArray> Replies = new Dictionary<long, DynamicJsonArray>();
    public readonly Dictionary<long, ClusterTransactionCommand.ClusterTransactionOptions> Options = new Dictionary<long, ClusterTransactionCommand.ClusterTransactionOptions>();

    public ClusterTransactionMergedCommand(DocumentDatabase database, ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand> batch) : base(database)
    {
        _batch = batch;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var global = context.LastDatabaseChangeVector ??
                     (context.LastDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context));
        var current = ChangeVectorUtils.GetEtagById(global, Database.DatabaseGroupId);

        Replies.Clear();
        Options.Clear();

        foreach (var command in _batch)
        {
            Replies.Add(command.Index, new DynamicJsonArray());
            Reply = Replies[command.Index];

            var commands = command.Commands;
            var count = command.PreviousCount;
            var options = Options[command.Index] = command.Options;

            if (options.WaitForIndexesTimeout != null)
            {
                ModifiedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }

            if (commands != null)
            {
                foreach (BlittableJsonReaderObject blittableCommand in commands)
                {
                    count++;
                    var changeVector = ChangeVectorUtils.GetClusterWideChangeVector(Database.DatabaseGroupId, count, options.DisableAtomicDocumentWrites == false, command.Index, Database.ClusterTransactionId);
                    var cmd = JsonDeserializationServer.ClusterTransactionDataCommand(blittableCommand);

                    switch (cmd.Type)
                    {
                        case CommandType.PUT:
                            if (current < count)
                            {
                                // delete the document to avoid exception if we put new document in a different collection.
                                // TODO: document this behavior
                                using (DocumentIdWorker.GetSliceFromId(context, cmd.Id, out Slice lowerId))
                                {
                                    Database.DocumentsStorage.Delete(context, lowerId, cmd.Id, expectedChangeVector: null,
                                        nonPersistentFlags: NonPersistentDocumentFlags.SkipRevisionCreation);
                                }

                                var putResult = Database.DocumentsStorage.Put(context, cmd.Id, null, cmd.Document.Clone(context), changeVector: changeVector,
                                    flags: DocumentFlags.FromClusterTransaction);
                                context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                                AddPutResult(putResult);
                            }
                            else
                            {
                                try
                                {
                                    var item = Database.DocumentsStorage.GetDocumentOrTombstone(context, cmd.Id);
                                    if (item.Missing)
                                    {
                                        AddPutResult(new DocumentsStorage.PutOperationResults
                                        {
                                            ChangeVector = changeVector,
                                            Id = cmd.Id,
                                            LastModified = DateTime.UtcNow,
                                            Collection = Database.DocumentsStorage.ExtractCollectionName(context, cmd.Document)
                                        });
                                        continue;
                                    }
                                    var collection = GetCollection(context, item);
                                    AddPutResult(new DocumentsStorage.PutOperationResults
                                    {
                                        ChangeVector = changeVector,
                                        Id = cmd.Id,
                                        Flags = item.Document?.Flags ?? item.Tombstone.Flags,
                                        LastModified = item.Document?.LastModified ?? item.Tombstone.LastModified,
                                        Collection = collection
                                    });
                                }
                                catch (DocumentConflictException)
                                {
                                    AddPutResult(new DocumentsStorage.PutOperationResults
                                    {
                                        ChangeVector = changeVector,
                                        Id = cmd.Id,
                                        Collection = GetFirstConflictCollection(context, cmd)
                                    });
                                }
                            }

                            break;

                        case CommandType.DELETE:
                            if (current < count)
                            {
                                using (DocumentIdWorker.GetSliceFromId(context, cmd.Id, out Slice lowerId))
                                {
                                    var deleteResult = Database.DocumentsStorage.Delete(context, lowerId, cmd.Id, null, changeVector: changeVector,
                                        documentFlags: DocumentFlags.FromClusterTransaction);
                                    AddDeleteResult(deleteResult, cmd.Id);
                                }
                            }
                            else
                            {
                                try
                                {
                                    var item = Database.DocumentsStorage.GetDocumentOrTombstone(context, cmd.Id);
                                    if (item.Missing)
                                    {
                                        AddDeleteResult(new DocumentsStorage.DeleteOperationResult
                                        {
                                            ChangeVector = changeVector,
                                            Collection = null
                                        }, cmd.Id);
                                        continue;
                                    }
                                    var collection = GetCollection(context, item);
                                    AddDeleteResult(new DocumentsStorage.DeleteOperationResult
                                    {
                                        ChangeVector = changeVector,
                                        Collection = collection
                                    }, cmd.Id);
                                }
                                catch (DocumentConflictException)
                                {
                                    AddDeleteResult(new DocumentsStorage.DeleteOperationResult
                                    {
                                        ChangeVector = changeVector,
                                        Collection = GetFirstConflictCollection(context, cmd)
                                    }, cmd.Id);
                                }
                            }
                            break;

                        default:
                            throw new NotSupportedException($"{cmd.Type} is not supported in {nameof(ClusterTransactionMergedCommand)}.");
                    }
                }
            }

            if (context.LastDatabaseChangeVector == null)
            {
                context.LastDatabaseChangeVector = global;
            }

            global.UpdateOrder(ChangeVectorParser.RaftTag, Database.DatabaseGroupId, count);
            context.LastDatabaseChangeVector = global;
        }

        return Reply.Count;
    }

    private CollectionName GetCollection(DocumentsOperationContext context, DocumentsStorage.DocumentOrTombstone item)
    {
        return item.Document != null
            ? Database.DocumentsStorage.ExtractCollectionName(context, item.Document.Data)
            : Database.DocumentsStorage.ExtractCollectionName(context, item.Tombstone.Collection);
    }

    private CollectionName GetFirstConflictCollection(DocumentsOperationContext context, ClusterTransactionCommand.ClusterTransactionDataCommand cmd)
    {
        var conflicts = Database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, cmd.Id);
        if (conflicts.Count == 0)
            return null;
        return Database.DocumentsStorage.ExtractCollectionName(context, conflicts[0].Collection);
    }

    public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
    {
        return new ClusterTransactionMergedCommandDto
        {
            Batch = _batch.Slice(0, _batch.Count).ToList()
        };
    }

    public override void Dispose()
    {
    }
}
