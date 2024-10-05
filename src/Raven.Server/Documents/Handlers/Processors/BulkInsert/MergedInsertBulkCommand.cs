using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron.Exceptions;

namespace Raven.Server.Documents.Handlers.Processors.BulkInsert;

public sealed class MergedInsertBulkCommand : DocumentMergedTransactionCommand
{
    public RavenLogger Logger;
    public DocumentDatabase Database;
    public BatchRequestParser.CommandData[] Commands;
    public int NumberOfCommands;
    public long TotalSize;
    public bool SkipOverwriteIfUnchanged;

    private readonly Dictionary<string, DocumentUpdates> _documentsToUpdate = new Dictionary<string, DocumentUpdates>(StringComparer.OrdinalIgnoreCase);

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        for (int i = 0; i < NumberOfCommands; i++)
        {
            var cmd = Commands[i];

            Debug.Assert(cmd.Type == CommandType.PUT || cmd.Type == CommandType.Counters || cmd.Type == CommandType.TimeSeries || cmd.Type == CommandType.TimeSeriesBulkInsert || cmd.Type == CommandType.AttachmentPUT);

            switch (cmd.Type)
            {
                case CommandType.PUT:
                    try
                    {
                        if (SkipOverwriteIfUnchanged)
                        {
                            var existingDoc = Database.DocumentsStorage.Get(context, cmd.Id, DocumentFields.Data, throwOnConflict: false);
                            if (existingDoc != null)
                            {
                                var compareResult = DocumentCompare.IsEqualTo(existingDoc.Data, cmd.Document,
                                    DocumentCompare.DocumentCompareOptions.MergeMetadata);

                                if (compareResult.HasFlag(DocumentCompareResult.Equal))
                                {
                                    Debug.Assert(BitOperations.PopCount((ulong)compareResult) == 1 ||
                                                 compareResult.HasFlag(DocumentCompareResult.AttachmentsNotEqual) ||
                                                 compareResult.HasFlag(DocumentCompareResult.CountersNotEqual) ||
                                                 compareResult.HasFlag(DocumentCompareResult.TimeSeriesNotEqual));
                                    continue;
                                }
                            }
                        }

                        Database.DocumentsStorage.Put(context, cmd.Id, null, cmd.Document);
                    }
                    catch (VoronConcurrencyErrorException)
                    {
                        // RavenDB-10581 - If we have a concurrency error on "doc-id/"
                        // this means that we have existing values under the current etag
                        // we'll generate a new (random) id for them.

                        // The TransactionMerger will re-run us when we ask it to as a
                        // separate transaction

                        for (; i < NumberOfCommands; i++)
                        {
                            cmd = Commands[i];
                            if (cmd.Type != CommandType.PUT)
                                continue;

                            if (cmd.Id?.EndsWith(Database.IdentityPartsSeparator) == true)
                            {
                                cmd.Id = MergedPutCommand.GenerateNonConflictingId(Database, cmd.Id);
                                RetryOnError = true;
                            }
                        }

                        throw;
                    }

                    break;

                case CommandType.Counters:
                    {
                        var collection = CountersHandler.ExecuteCounterBatchCommand.GetDocumentCollection(cmd.Id, Database, context, fromEtl: false, out _);

                        foreach (var counterOperation in cmd.Counters.Operations)
                        {
                            counterOperation.DocumentId = cmd.Counters.DocumentId;
                            Database.DocumentsStorage.CountersStorage.IncrementCounter(context, cmd.Id, collection, counterOperation.CounterName, counterOperation.Delta, out _);

                            var updates = GetDocumentUpdates(cmd.Id);
                            updates.AddCounter(counterOperation.CounterName);
                        }

                        break;
                    }
                case CommandType.TimeSeries:
                case CommandType.TimeSeriesBulkInsert:
                    {
                        var docCollection = TimeSeriesHandler.ExecuteTimeSeriesBatchCommand.GetDocumentCollection(Database, context, cmd.Id, fromEtl: false);
                        Database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context,
                            cmd.Id,
                            docCollection,
                            cmd.TimeSeries.Name,
                            cmd.TimeSeries.Appends
                        );
                        break;
                    }
                case CommandType.AttachmentPUT:
                    {
                        using (cmd.AttachmentStream.Stream)
                        {
                            Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, cmd.Id, cmd.Name,
                                cmd.ContentType ?? "", cmd.AttachmentStream.Hash, cmd.ChangeVector, cmd.AttachmentStream.Stream, updateDocument: false);
                        }

                        var updates = GetDocumentUpdates(cmd.Id);
                        updates.AddAttachment();

                        break;
                    }
            }
        }

        if (_documentsToUpdate.Count > 0)
        {
            foreach (var kvp in _documentsToUpdate)
            {
                var documentId = kvp.Key;
                var updates = kvp.Value;

                if (updates.Attachments)
                    Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, documentId);

                if (updates.Counters != null && updates.Counters.Count > 0)
                {
                    var docToUpdate = Database.DocumentsStorage.Get(context, documentId);
                    if (docToUpdate != null)
                    {
                        Database.DocumentsStorage.CountersStorage.UpdateDocumentCounters(context, docToUpdate, documentId, updates.Counters, countersToRemove: null, NonPersistentDocumentFlags.ByCountersUpdate);
                    }
                }
            }
        }

        if (Logger.IsInfoEnabled)
        {
            Logger.Info($"Executed {NumberOfCommands:#,#;;0} bulk insert operations, size: ({new Size(TotalSize, SizeUnit.Bytes)})");
        }

        return NumberOfCommands;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        return new MergedInsertBulkCommandDto
        {
            Commands = Commands.Take(NumberOfCommands).ToArray()
        };
    }

    private DocumentUpdates GetDocumentUpdates(string documentId)
    {
        if (_documentsToUpdate.TryGetValue(documentId, out var update) == false)
            _documentsToUpdate[documentId] = update = new DocumentUpdates();

        return update;
    }

    private sealed class DocumentUpdates
    {
        public bool Attachments;

        public SortedSet<string> Counters;

        public void AddCounter(string counterName)
        {
            if (Counters == null)
                Counters = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            Counters.Add(counterName);
        }

        public void AddAttachment()
        {
            Attachments = true;
        }
    }
}
