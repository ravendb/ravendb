using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

public sealed class MergedBatchCommand : TransactionMergedCommand
{
    public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;

    public List<AttachmentStream> AttachmentStreams;
    public StreamsTempFile AttachmentStreamsTempFile;

    private Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>> _documentsToUpdateAfterAttachmentChange;
    private readonly List<IDisposable> _toDispose = new();
    private static TimeSeriesStorage.AppendOptions AppendOptionsForTimeSeriesCopy = new() { VerifyName = false };

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
                                                                           nonPersistentFlags: NonPersistentDocumentFlags.ForceRevisionCreation,
                                                                           context.GetChangeVector(existingDocument.ChangeVector),
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

                    context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(putResult.Id, cmd.Document.Size);
                    AddPutResult(putResult);
                    lastPutResult = putResult;
                    break;

                case CommandType.PATCH:
                case CommandType.BatchPATCH:
                    cmd.PatchCommand.ExecuteDirectly(context);

                    var lastChangeVector = cmd.PatchCommand.HandleReply(IncludeReply ? Reply : null, ModifiedCollections);
                    if (lastChangeVector != null)
                        LastChangeVector = lastChangeVector;

                    break;

                case CommandType.JsonPatch:
                    cmd.JsonPatchCommand.ExecuteDirectly(context);

                    var lastChangeVectorJsonPatch = cmd.JsonPatchCommand.HandleReply(IncludeReply ? Reply : null, ModifiedCollections, Database);
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
                    var docId = EtlGetDocIdFromPrefixIfNeeded(cmd.Id, cmd, lastPutResult);

                    //TODO: egor do here something normal and don't pass so many params and flags 

                    AttachmentDetailsServer attachmentPutResult;
                    if (cmd.FromEtl)
                    {
                        if (cmd.Flags.Contain(AttachmentFlags.Retired) == false)
                        {
                            AttachmentStream attachmentStream = GetAttachmentStream(attachmentIterator, out Stream stream);
                            attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, docId, cmd.Name,
                                cmd.ContentType, attachmentStream.Hash, cmd.Flags, cmd.Size, cmd.RetiredAt, cmd.ChangeVector, stream, updateDocument: false, extractCollectionName: ModifiedCollections is not null, fromEtl: cmd.FromEtl);
                        }
                        else
                        {
                            attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, docId, cmd.Name,
                                cmd.ContentType, cmd.Hash, cmd.Flags, cmd.Size, cmd.RetiredAt, cmd.ChangeVector, stream: null, updateDocument: false, extractCollectionName: ModifiedCollections is not null, fromEtl: cmd.FromEtl);
                        }
                    }
                    else
                    {
                        AttachmentStream attachmentStream = GetAttachmentStream(attachmentIterator, out Stream stream);
                        attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, docId, cmd.Name,
                            cmd.ContentType, attachmentStream.Hash, flags: AttachmentFlags.None, stream.Length, retireAtDt: null, cmd.ChangeVector, stream, updateDocument: false, extractCollectionName: ModifiedCollections is not null);
                    }

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

                    if (attachmentPutResult.CollectionName != null)
                        ModifiedCollections?.Add(attachmentPutResult.CollectionName.Name);

                    _documentsToUpdateAfterAttachmentChange ??= new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                    if (_documentsToUpdateAfterAttachmentChange.TryGetValue(docId, out var apReplies) == false)
                        _documentsToUpdateAfterAttachmentChange[docId] = apReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                    apReplies.Add((apReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));

                    if (IncludeReply == false)
                        break;

                    Reply.Add(apReply);

                    break;

                case CommandType.AttachmentDELETE:

                    bool storageOnly = Database.ReadDatabaseRecord().RetiredAttachments is not { Disabled: false, PurgeOnDelete: true };
                    Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, cmd.Id, cmd.Name, cmd.ChangeVector, out var collectionName, updateDocument: false, extractCollectionName: ModifiedCollections is not null, storageOnly: storageOnly);

                    if (collectionName != null)
                        ModifiedCollections?.Add(collectionName.Name);

                    var adReply = new DynamicJsonValue
                    {
                        ["Type"] = nameof(CommandType.AttachmentDELETE),
                        [Constants.Documents.Metadata.Id] = cmd.Id,
                        ["Name"] = cmd.Name
                    };

                    _documentsToUpdateAfterAttachmentChange ??= new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                    if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.Id, out var adReplies) == false)
                        _documentsToUpdateAfterAttachmentChange[cmd.Id] = adReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                    adReplies.Add((adReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));

                    if (IncludeReply == false)
                        break;

                    Reply.Add(adReply);

                    break;

                case CommandType.AttachmentMOVE:
                    var attachmentMoveOutput = Database.DocumentsStorage.AttachmentsStorage.MoveAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector, extractCollectionName: ModifiedCollections is not null);
                    var attachmentMoveResult = attachmentMoveOutput.Result;

                    if (attachmentMoveOutput.DestinationCollectionName != null)
                        ModifiedCollections?.Add(attachmentMoveOutput.DestinationCollectionName.Name);
                    if (attachmentMoveOutput.SourceCollectionName != null)
                        ModifiedCollections?.Add(attachmentMoveOutput.SourceCollectionName.Name);

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

                    _documentsToUpdateAfterAttachmentChange ??= new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                    if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.Id, out var amReplies) == false)
                        _documentsToUpdateAfterAttachmentChange[cmd.Id] = amReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                    amReplies.Add((amReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));

                    if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.DestinationId, out amReplies) == false)
                        _documentsToUpdateAfterAttachmentChange[cmd.DestinationId] = amReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                    amReplies.Add((amReply, nameof(Constants.Fields.CommandData.DestinationDocumentChangeVector)));

                    if (IncludeReply == false)
                        break;

                    Reply.Add(amReply);
                    break;

                case CommandType.AttachmentCOPY:
                    if (cmd.AttachmentType == 0)
                    {
                        // if attachment type is not sent, we fallback to default, which is Document
                        cmd.AttachmentType = AttachmentType.Document;
                    }
                    var attachmentCopyResult = Database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector, cmd.AttachmentType, extractCollectionName: ModifiedCollections is not null);

                    if (attachmentCopyResult.CollectionName != null)
                        ModifiedCollections?.Add(attachmentCopyResult.CollectionName.Name);

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

                    _documentsToUpdateAfterAttachmentChange ??= new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                    if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.DestinationId, out var acReplies) == false)
                        _documentsToUpdateAfterAttachmentChange[cmd.DestinationId] = acReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                    acReplies.Add((acReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));

                    if (IncludeReply == false)
                        break;

                    Reply.Add(acReply);

                    break;

                case CommandType.TimeSeries:
                case CommandType.TimeSeriesWithIncrements:
                    cmd.Id = EtlGetDocIdFromPrefixIfNeeded(cmd.Id, cmd, lastPutResult);
                    var tsCmd = new TimeSeriesHandler.ExecuteTimeSeriesBatchCommand(Database, cmd.Id, cmd.TimeSeries, cmd.FromEtl);

                    tsCmd.ExecuteDirectly(context);

                    LastChangeVector = tsCmd.LastChangeVector;

                    if (tsCmd.DocCollection != null)
                        ModifiedCollections?.Add(tsCmd.DocCollection);

                    if (IncludeReply == false)
                        break;

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

                    var destinationDocCollection = TimeSeriesHandler.ExecuteTimeSeriesBatchCommand.GetDocumentCollection(Database, context, cmd.DestinationId, fromEtl: false);

                    var cv = Database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context,
                            cmd.DestinationId,
                            destinationDocCollection,
                            cmd.DestinationName,
                            reader.AllValues(),
                            AppendOptionsForTimeSeriesCopy
                        );

                    LastChangeVector = cv;

                    ModifiedCollections?.Add(destinationDocCollection);

                    if (IncludeReply == false)
                        break;

                    Reply.Add(new DynamicJsonValue
                    {
                        [nameof(BatchRequestParser.CommandData.Id)] = cmd.DestinationId,
                        [nameof(BatchRequestParser.CommandData.ChangeVector)] = cv,
                        [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.TimeSeriesCopy),
                    });

                    break;

                case CommandType.Counters:
                    cmd.Counters.DocumentId = EtlGetDocIdFromPrefixIfNeeded(cmd.Counters.DocumentId, cmd, lastPutResult);

                    var counterBatchCmd = new CountersHandler.ExecuteCounterBatchCommand(Database, new CounterBatch
                    {
                        Documents = new List<DocumentCountersOperation> { cmd.Counters },
                        FromEtl = cmd.FromEtl
                    });
                    counterBatchCmd.ExecuteDirectly(context);

                    LastChangeVector = counterBatchCmd.LastChangeVector;

                    if (counterBatchCmd.DocumentCollections != null)
                    {
                        foreach (var collection in counterBatchCmd.DocumentCollections)
                        {
                            ModifiedCollections?.Add(collection);
                        }
                    }

                    if (IncludeReply == false)
                        break;

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

                    // Note: here there is no point checking 'Before' or 'After' because if there were any changes then the forced revision is done from the PUT command....

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
                    bool revisionCreated = Database.DocumentsStorage.RevisionsStorage.Put(context, existingDoc.Id,
                                                                                 clonedDocData,
                                                                                 existingDoc.Flags,
                                                                                 nonPersistentFlags: NonPersistentDocumentFlags.ForceRevisionCreation,
                                                                                 context.GetChangeVector(existingDoc.ChangeVector),
                                                                                 existingDoc.LastModified.Ticks);

                    if (revisionCreated)
                    {
                        LastChangeVector = existingDoc.ChangeVector;

                        if (IncludeReply == false)
                            break;

                        // Reply with new revision data
                        Reply.Add(new DynamicJsonValue
                        {
                            ["RevisionCreated"] = true,
                            ["Type"] = nameof(CommandType.ForceRevisionCreation),
                            [Constants.Documents.Metadata.Id] = existingDoc.Id.ToString(), //We must not return to handlers memory allocated by merger.
                            [Constants.Documents.Metadata.ChangeVector] = existingDoc.ChangeVector,
                            [Constants.Documents.Metadata.LastModified] = existingDoc.LastModified,
                            [Constants.Documents.Metadata.Flags] = existingDoc.Flags
                        });
                    }
                    else
                    {
                        if (IncludeReply == false)
                            break;

                        // No revision was created
                        Reply.Add(new DynamicJsonValue
                        {
                            ["RevisionCreated"] = false,
                            ["Type"] = nameof(CommandType.ForceRevisionCreation)
                        });
                    }

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

        // We are requested to do not return result.
        if (IncludeReply == false)
            Debug.Assert(Reply.Count == 0);

        return Reply.Count;
    }

    private AttachmentStream GetAttachmentStream(IEnumerator<AttachmentStream> attachmentIterator, out Stream stream)
    {
        attachmentIterator.MoveNext();
        var attachmentStream = attachmentIterator.Current;
        stream = attachmentStream.Stream;
        _toDispose.Add(stream);
        return attachmentStream;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        return new MergedBatchCommandDto
        {
            ParsedCommands = ParsedCommands.ToArray(),
            AttachmentStreams = AttachmentStreams,
            IncludeReply = IncludeReply
        };
    }

    private string EtlGetDocIdFromPrefixIfNeeded(string docId, BatchRequestParser.CommandData cmd, DocumentsStorage.PutOperationResults? lastPutResult)
    {
        if (cmd.FromEtl == false || docId[^1] != Database.IdentityPartsSeparator)
            return docId;
        // counter/time-series/attachments sent by Raven ETL, only prefix is defined

        if (lastPutResult == null)
            ThrowUnexpectedOrderOfRavenEtlCommands("Raven ETL");

        Debug.Assert(lastPutResult.HasValue && lastPutResult.Value.Id.StartsWith(docId));
        return lastPutResult.Value.Id;
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

    [DoesNotReturn]
    private void ThrowUnexpectedOrderOfRavenEtlCommands(string source)
    {
        throw new InvalidOperationException($"Unexpected order of commands sent by {source}. {CommandType.AttachmentPUT} needs to be preceded by {CommandType.PUT}");
    }
}
