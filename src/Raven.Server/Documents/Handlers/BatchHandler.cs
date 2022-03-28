using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Handlers
{
    public class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkDocs()
        {
            using (var processor = new BatchHandlerProcessorForBulkDocs(this))
                await processor.ExecuteAsync();
        }

        public interface IBatchCommand : IDisposable
        {
            public HashSet<string> ModifiedCollections { get; set; }

            public string LastChangeVector { get; set; }

            public long LastTombstoneEtag { get; set; }

            public bool IsClusterTransaction { get; set; }
        }

        public abstract class TransactionMergedCommand : TransactionOperationsMerger.MergedTransactionCommand, IBatchCommand
        {
            protected readonly DocumentDatabase Database;

            public DynamicJsonArray Reply = new();

            public HashSet<string> ModifiedCollections { get; set; }

            public string LastChangeVector { get; set; }

            public long LastTombstoneEtag { get; set; }

            public bool IsClusterTransaction { get; set; }

            protected TransactionMergedCommand(DocumentDatabase database)
            {
                Database = database;
            }

            protected void AddPutResult(DocumentsStorage.PutOperationResults putResult)
            {
                LastChangeVector = putResult.ChangeVector;
                ModifiedCollections?.Add(putResult.Collection.Name);

                // Make sure all the metadata fields are always been add
                var putReply = new DynamicJsonValue
                {
                    ["Type"] = nameof(CommandType.PUT),
                    [Constants.Documents.Metadata.Id] = putResult.Id,
                    [Constants.Documents.Metadata.Collection] = putResult.Collection.Name,
                    [Constants.Documents.Metadata.ChangeVector] = putResult.ChangeVector,
                    [Constants.Documents.Metadata.LastModified] = putResult.LastModified
                };

                if (putResult.Flags != DocumentFlags.None)
                    putReply[Constants.Documents.Metadata.Flags] = putResult.Flags;

                Reply.Add(putReply);
            }

            protected void AddDeleteResult(DocumentsStorage.DeleteOperationResult? deleted, string id)
            {
                var reply = new DynamicJsonValue
                {
                    [nameof(BatchRequestParser.CommandData.Id)] = id,
                    [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                    ["Deleted"] = deleted != null
                };

                if (deleted != null)
                {
                    if (deleted.Value.Collection != null)
                        ModifiedCollections?.Add(deleted.Value.Collection.Name);

                    LastTombstoneEtag = deleted.Value.Etag;
                    reply[nameof(BatchRequestParser.CommandData.ChangeVector)] = deleted.Value.ChangeVector;
                }

                Reply.Add(reply);
            }

            protected void DeleteWithPrefix(DocumentsOperationContext context, string id)
            {
                var deleteResults = Database.DocumentsStorage.DeleteDocumentsStartingWith(context, id);

                var deleted = deleteResults.Count > 0;
                if (deleted)
                {
                    LastChangeVector = deleteResults[deleteResults.Count - 1].ChangeVector;
                    for (var j = 0; j < deleteResults.Count; j++)
                    {
                        ModifiedCollections?.Add(deleteResults[j].Collection.Name);
                    }
                }

                Reply.Add(new DynamicJsonValue
                {
                    [nameof(BatchRequestParser.CommandData.Id)] = id,
                    [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                    ["Deleted"] = deleted
                });
            }

            public abstract void Dispose();
        }

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

                    var updatedChangeVector = ChangeVectorUtils.TryUpdateChangeVector("RAFT", Database.DatabaseGroupId, count, global);

                    if (updatedChangeVector.IsValid)
                    {
                        context.LastDatabaseChangeVector = updatedChangeVector.ChangeVector;
                    }
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

        public class MergedBatchCommand : TransactionMergedCommand
        {
            public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;

            public List<AttachmentStream> AttachmentStreams;
            public StreamsTempFile AttachmentStreamsTempFile;

            private Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>> _documentsToUpdateAfterAttachmentChange;
            private readonly List<IDisposable> _toDispose = new();

            public MergedBatchCommand(DocumentDatabase database) : base(database)
            {
            }

            public override string ToString()
            {
                var sb = new StringBuilder($"{ParsedCommands.Count} commands").AppendLine();
                if (AttachmentStreams != null)
                {
                    sb.AppendLine($"{AttachmentStreams.Count} attachment streams.");
                }

                foreach (var cmd in ParsedCommands)
                {
                    sb.Append("\t")
                        .Append(cmd.Type)
                        .Append(" ")
                        .Append(cmd.Id)
                        .AppendLine();
                }

                return sb.ToString();
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedBatchCommandDto
                {
                    ParsedCommands = ParsedCommands.ToArray(),
                    AttachmentStreams = AttachmentStreams
                };
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (IsClusterTransaction)
                {
                    Debug.Assert(false, "Shouldn't happen - cluster tx run via normal means");
                    return 0;// should never happened
                }
                Reply.Clear();
                _toDispose.Clear();

                DocumentsStorage.PutOperationResults? lastPutResult = null;

                using IEnumerator<AttachmentStream> attachmentIterator = AttachmentStreams?.GetEnumerator();

                for (int i = ParsedCommands.Offset; i < ParsedCommands.Count; i++)
                {
                    var cmd = ParsedCommands.Array[i];

                    switch (cmd.Type)
                    {
                        case CommandType.PUT:

                            DocumentsStorage.PutOperationResults putResult;
                            try
                            {
                                var flags = DocumentFlags.None;

                                if (cmd.ForceRevisionCreationStrategy == ForceRevisionStrategy.Before)
                                // Note: we currently only handle 'Before'.
                                // Creating the revision 'After' will be done only upon customer demand.
                                {
                                    var existingDocument = Database.DocumentsStorage.Get(context, cmd.Id);
                                    if (existingDocument == null)
                                    {
                                        throw new InvalidOperationException($"Can't force revision creation - the document was not saved on the server yet. document: {cmd.Id}.");
                                    }

                                    // Force a revision (before applying new document changes..)
                                    Database.DocumentsStorage.RevisionsStorage.Put(context, existingDocument.Id,
                                                                                   existingDocument.Data.Clone(context),
                                                                                   existingDocument.Flags |= DocumentFlags.HasRevisions,
                                                                                   nonPersistentFlags: NonPersistentDocumentFlags.None,
                                                                                   existingDocument.ChangeVector,
                                                                                   existingDocument.LastModified.Ticks);
                                    flags |= DocumentFlags.HasRevisions;
                                }

                                putResult = Database.DocumentsStorage.Put(context, cmd.Id, cmd.ChangeVector, cmd.Document,
                                    oldChangeVectorForClusterTransactionIndexCheck: cmd.OriginalChangeVector, flags: flags);
                            }
                            catch (Voron.Exceptions.VoronConcurrencyErrorException)
                            {
                                // RavenDB-10581 - If we have a concurrency error on "doc-id/"
                                // this means that we have existing values under the current etag
                                // we'll generate a new (random) id for them.

                                // The TransactionMerger will re-run us when we ask it to as a
                                // separate transaction
                                for (; i < ParsedCommands.Count; i++)
                                {
                                    cmd = ParsedCommands.Array[ParsedCommands.Offset + i];
                                    if (cmd.Type == CommandType.PUT && cmd.Id?.EndsWith(Database.IdentityPartsSeparator) == true)
                                    {
                                        cmd.Id = MergedPutCommand.GenerateNonConflictingId(Database, cmd.Id);
                                        RetryOnError = true;
                                    }
                                }
                                throw;
                            }

                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                            AddPutResult(putResult);
                            lastPutResult = putResult;
                            break;

                        case CommandType.PATCH:
                        case CommandType.BatchPATCH:
                            cmd.PatchCommand.ExecuteDirectly(context);

                            var lastChangeVector = cmd.PatchCommand.HandleReply(Reply, ModifiedCollections);

                            if (lastChangeVector != null)
                                LastChangeVector = lastChangeVector;

                            break;

                        case CommandType.JsonPatch:

                            cmd.JsonPatchCommand.ExecuteDirectly(context);

                            var lastChangeVectorJsonPatch = cmd.JsonPatchCommand.HandleReply(Reply, ModifiedCollections, Database);

                            if (lastChangeVectorJsonPatch != null)
                                LastChangeVector = lastChangeVectorJsonPatch;
                            break;

                        case CommandType.DELETE:
                            if (cmd.IdPrefixed == false)
                            {
                                var deleted = Database.DocumentsStorage.Delete(context, cmd.Id, cmd.ChangeVector);
                                AddDeleteResult(deleted, cmd.Id);
                            }
                            else
                            {
                                DeleteWithPrefix(context, cmd.Id);
                            }
                            break;

                        case CommandType.AttachmentPUT:
                            attachmentIterator.MoveNext();
                            var attachmentStream = attachmentIterator.Current;
                            var stream = attachmentStream.Stream;
                            _toDispose.Add(stream);

                            var docId = cmd.Id;

                            if (docId[docId.Length - 1] == Database.IdentityPartsSeparator)
                            {
                                // attachment sent by Raven ETL, only prefix is defined

                                if (lastPutResult == null)
                                    ThrowUnexpectedOrderOfRavenEtlCommands();

                                Debug.Assert(lastPutResult.Value.Id.StartsWith(docId));

                                docId = lastPutResult.Value.Id;
                            }

                            var attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, docId, cmd.Name,
                                cmd.ContentType, attachmentStream.Hash, cmd.ChangeVector, stream, updateDocument: false);
                            LastChangeVector = attachmentPutResult.ChangeVector;

                            var apReply = new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = attachmentPutResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentPUT),
                                [nameof(BatchRequestParser.CommandData.Name)] = attachmentPutResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentPutResult.ChangeVector,
                                [nameof(AttachmentDetails.Hash)] = attachmentPutResult.Hash,
                                [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentPutResult.ContentType,
                                [nameof(AttachmentDetails.Size)] = attachmentPutResult.Size
                            };

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(docId, out var apReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[docId] = apReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            apReplies.Add((apReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));
                            Reply.Add(apReply);
                            break;

                        case CommandType.AttachmentDELETE:
                            Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, cmd.Id, cmd.Name, cmd.ChangeVector, updateDocument: false);

                            var adReply = new DynamicJsonValue
                            {
                                ["Type"] = nameof(CommandType.AttachmentDELETE),
                                [Constants.Documents.Metadata.Id] = cmd.Id,
                                ["Name"] = cmd.Name
                            };

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.Id, out var adReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.Id] = adReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            adReplies.Add((adReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));
                            Reply.Add(adReply);
                            break;

                        case CommandType.AttachmentMOVE:
                            var attachmentMoveResult = Database.DocumentsStorage.AttachmentsStorage.MoveAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector);

                            LastChangeVector = attachmentMoveResult.ChangeVector;

                            var amReply = new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentMOVE),
                                [nameof(BatchRequestParser.CommandData.Name)] = cmd.Name,
                                [nameof(BatchRequestParser.CommandData.DestinationId)] = attachmentMoveResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.DestinationName)] = attachmentMoveResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentMoveResult.ChangeVector,
                                [nameof(AttachmentDetails.Hash)] = attachmentMoveResult.Hash,
                                [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentMoveResult.ContentType,
                                [nameof(AttachmentDetails.Size)] = attachmentMoveResult.Size
                            };

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.Id, out var amReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.Id] = amReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            amReplies.Add((amReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.DestinationId, out amReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.DestinationId] = amReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            amReplies.Add((amReply, nameof(Constants.Fields.CommandData.DestinationDocumentChangeVector)));

                            Reply.Add(amReply);
                            break;

                        case CommandType.AttachmentCOPY:
                            if (cmd.AttachmentType == 0)
                            {
                                // if attachment type is not sent, we fallback to default, which is Document
                                cmd.AttachmentType = AttachmentType.Document;
                            }
                            var attachmentCopyResult = Database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector, cmd.AttachmentType);

                            LastChangeVector = attachmentCopyResult.ChangeVector;

                            var acReply = new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = attachmentCopyResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentCOPY),
                                [nameof(BatchRequestParser.CommandData.Name)] = attachmentCopyResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentCopyResult.ChangeVector,
                                [nameof(AttachmentDetails.Hash)] = attachmentCopyResult.Hash,
                                [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentCopyResult.ContentType,
                                [nameof(AttachmentDetails.Size)] = attachmentCopyResult.Size
                            };

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.DestinationId, out var acReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.DestinationId] = acReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            acReplies.Add((acReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));
                            Reply.Add(acReply);
                            break;

                        case CommandType.TimeSeries:
                        case CommandType.TimeSeriesWithIncrements:
                            EtlGetDocIdFromPrefixIfNeeded(ref cmd.Id, cmd, lastPutResult);
                            var tsCmd = new TimeSeriesHandler.ExecuteTimeSeriesBatchCommand(Database, cmd.Id, cmd.TimeSeries, cmd.FromEtl);

                            tsCmd.ExecuteDirectly(context);

                            LastChangeVector = tsCmd.LastChangeVector;

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = tsCmd.LastChangeVector,
                                [nameof(BatchRequestParser.CommandData.Type)] = cmd.Type,
                                //TODO: handle this
                                //[nameof(Constants.Fields.CommandData.DocumentChangeVector)] = tsCmd.LastDocumentChangeVector
                            });

                            break;

                        case CommandType.TimeSeriesCopy:

                            var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(context, cmd.Id, cmd.Name, cmd.From ?? DateTime.MinValue, cmd.To ?? DateTime.MaxValue);

                            var docCollection = TimeSeriesHandler.ExecuteTimeSeriesBatchCommand.GetDocumentCollection(Database, context, cmd.DestinationId, fromEtl: false);

                            var cv = Database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context,
                                    cmd.DestinationId,
                                    docCollection,
                                    cmd.DestinationName,
                                    reader.AllValues()
                                );

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.DestinationId,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = cv,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.TimeSeriesCopy),
                            });
                            break;

                        case CommandType.Counters:
                            EtlGetDocIdFromPrefixIfNeeded(ref cmd.Counters.DocumentId, cmd, lastPutResult);

                            var counterBatchCmd = new CountersHandler.ExecuteCounterBatchCommand(Database, new CounterBatch
                            {
                                Documents = new List<DocumentCountersOperation> { cmd.Counters },
                                FromEtl = cmd.FromEtl
                            });
                            counterBatchCmd.ExecuteDirectly(context);

                            LastChangeVector = counterBatchCmd.LastChangeVector;

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = counterBatchCmd.LastChangeVector,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.Counters),
                                [nameof(CountersDetail)] = counterBatchCmd.CountersDetail.ToJson(),
                                [nameof(Constants.Fields.CommandData.DocumentChangeVector)] = counterBatchCmd.LastDocumentChangeVector
                            });
                            break;

                        case CommandType.ForceRevisionCreation:
                            // Create a revision for an existing document (specified by the id) even if revisions settings are Not configured for the collection.
                            // Only one such revision will be created.
                            // i.e. If there is already an existing 'forced' revision to this document then we don't create another forced revision.

                            var existingDoc = Database.DocumentsStorage.Get(context, cmd.Id);
                            if (existingDoc == null)
                            {
                                throw new InvalidOperationException($"Failed to create revision for document {cmd.Id} because document doesn't exits.");
                            }

                            DynamicJsonValue forceRevisionReply;

                            // Note: here there is no point checking 'Before' or 'After' because if there were any changes then the forced revision is done from the PUT command....

                            bool revisionCreated = false;
                            var clonedDocData = existingDoc.Data.Clone(context);

                            if (existingDoc.Flags.Contain(DocumentFlags.HasRevisions) == false)
                            {
                                // When forcing a revision for a document that doesn't have revisions yet,
                                // we must add HasRevisions flag to the document and save (put)
                                existingDoc.Flags = existingDoc.Flags |= DocumentFlags.HasRevisions;

                                putResult = Database.DocumentsStorage.Put(context, existingDoc.Id,
                                                                         existingDoc.ChangeVector,
                                                                          clonedDocData,
                                                                          flags: existingDoc.Flags,
                                                                          nonPersistentFlags: NonPersistentDocumentFlags.SkipRevisionCreation);

                                existingDoc.ChangeVector = putResult.ChangeVector;
                                existingDoc.LastModified = putResult.LastModified;
                            }

                            // Create the revision. Revision will be created only if there isn't a revision with identical content for this document
                            revisionCreated = Database.DocumentsStorage.RevisionsStorage.Put(context, existingDoc.Id,
                                                                                         clonedDocData,
                                                                                         existingDoc.Flags,
                                                                                         nonPersistentFlags: NonPersistentDocumentFlags.None,
                                                                                         existingDoc.ChangeVector,
                                                                                         existingDoc.LastModified.Ticks);
                            if (revisionCreated)
                            {
                                // Reply with new revision data
                                forceRevisionReply = new DynamicJsonValue
                                {
                                    ["RevisionCreated"] = true,
                                    ["Type"] = nameof(CommandType.ForceRevisionCreation),
                                    [Constants.Documents.Metadata.Id] = existingDoc.Id.ToString(), //We must not return to handlers memory allocated by merger.
                                    [Constants.Documents.Metadata.ChangeVector] = existingDoc.ChangeVector,
                                    [Constants.Documents.Metadata.LastModified] = existingDoc.LastModified,
                                    [Constants.Documents.Metadata.Flags] = existingDoc.Flags
                                };

                                LastChangeVector = existingDoc.ChangeVector;
                            }
                            else
                            {
                                // No revision was created
                                forceRevisionReply = new DynamicJsonValue
                                {
                                    ["RevisionCreated"] = false,
                                    ["Type"] = nameof(CommandType.ForceRevisionCreation)
                                };
                            }

                            Reply.Add(forceRevisionReply);
                            break;

                    }
                }

                if (_documentsToUpdateAfterAttachmentChange != null)
                {
                    foreach (var kvp in _documentsToUpdateAfterAttachmentChange)
                    {
                        var documentId = kvp.Key;
                        var changeVector = Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, documentId);

                        if (changeVector == null)
                            continue;

                        LastChangeVector = changeVector;

                        if (kvp.Value == null)
                            continue;

                        foreach (var tpl in kvp.Value)
                            tpl.Reply[tpl.FieldName] = changeVector;
                    }
                }
                return Reply.Count;
            }

            private void EtlGetDocIdFromPrefixIfNeeded(ref string docId, BatchRequestParser.CommandData cmd, DocumentsStorage.PutOperationResults? lastPutResult)
            {
                if (!cmd.FromEtl || docId[^1] != Database.IdentityPartsSeparator)
                    return;
                // counter/time-series sent by Raven ETL, only prefix is defined

                if (lastPutResult == null)
                    ThrowUnexpectedOrderOfRavenEtlCommands();

                Debug.Assert(lastPutResult.HasValue && lastPutResult.Value.Id.StartsWith(docId));
                docId = lastPutResult.Value.Id;
            }

            public override void Dispose()
            {
                if (ParsedCommands.Count == 0)
                    return;

                foreach (var disposable in _toDispose)
                {
                    disposable?.Dispose();
                }

                foreach (var cmd in ParsedCommands)
                {
                    cmd.Document?.Dispose();
                }
                BatchRequestParser.ReturnBuffer(ParsedCommands);
                AttachmentStreamsTempFile?.Dispose();
                AttachmentStreamsTempFile = null;
            }

            public struct AttachmentStream
            {
                public string Hash;
                public Stream Stream;
            }

            private void ThrowUnexpectedOrderOfRavenEtlCommands()
            {
                throw new InvalidOperationException($"Unexpected order of commands sent by Raven ETL. {CommandType.AttachmentPUT} needs to be preceded by {CommandType.PUT}");
            }
        }
    }

    public class ClusterTransactionMergedCommandDto : TransactionOperationsMerger.IReplayableCommandDto<BatchHandler.ClusterTransactionMergedCommand>
    {
        public List<ClusterTransactionCommand.SingleClusterDatabaseCommand> Batch { get; set; }

        public BatchHandler.ClusterTransactionMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new BatchHandler.ClusterTransactionMergedCommand(database, new ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand>(Batch.ToArray()));
            return command;
        }
    }

    public class MergedBatchCommandDto : TransactionOperationsMerger.IReplayableCommandDto<BatchHandler.MergedBatchCommand>
    {
        public BatchRequestParser.CommandData[] ParsedCommands { get; set; }
        public List<BatchHandler.MergedBatchCommand.AttachmentStream> AttachmentStreams;

        public BatchHandler.MergedBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            for (var i = 0; i < ParsedCommands.Length; i++)
            {
                if (ParsedCommands[i].Type != CommandType.PATCH)
                {
                    continue;
                }

                ParsedCommands[i].PatchCommand = new PatchDocumentCommand(
                    context: context,
                    id: ParsedCommands[i].Id,
                    expectedChangeVector: ParsedCommands[i].ChangeVector,
                    skipPatchIfChangeVectorMismatch: false,
                    patch: (ParsedCommands[i].Patch, ParsedCommands[i].PatchArgs),
                    patchIfMissing: (ParsedCommands[i].PatchIfMissing, ParsedCommands[i].PatchIfMissingArgs),
                    identityPartsSeparator: database.IdentityPartsSeparator,
                    createIfMissing: ParsedCommands[i].CreateIfMissing,
                    isTest: false,
                    debugMode: false,
                    collectResultsNeeded: true,
                    returnDocument: ParsedCommands[i].ReturnDocument
                );
            }

            var newCmd = new BatchHandler.MergedBatchCommand(database)
            {
                ParsedCommands = ParsedCommands,
                AttachmentStreams = AttachmentStreams
            };

            return newCmd;
        }
    }
}
