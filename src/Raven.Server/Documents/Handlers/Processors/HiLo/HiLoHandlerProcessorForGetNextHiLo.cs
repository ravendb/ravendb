﻿using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Identity;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.HiLo;

internal sealed class HiLoHandlerProcessorForGetNextHiLo : AbstractHiLoHandlerProcessorForGetNextHiLo<DatabaseRequestHandler, DocumentsOperationContext>
{
    public HiLoHandlerProcessorForGetNextHiLo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var tag = GetTag();
            var lastSize = GetLastBatchSize();
            var lastRangeAt = GetLastRangeAt();
            var identityPartsSeparator = GetIdentityPartsSeparator();
            var lastMax = GetLastMax();

            var capacity = CalculateCapacity(lastSize, lastRangeAt);

            var cmd = new MergedNextHiLoCommand
            {
                Database = RequestHandler.Database,
                Key = tag,
                Capacity = capacity,
                Separator = identityPartsSeparator,
                LastRangeMax = lastMax
            };

            await RequestHandler.Database.TxMerger.Enqueue(cmd);

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(HiLoResult.Prefix)] = cmd.Prefix,
                    [nameof(HiLoResult.Low)] = cmd.OldMax + 1,
                    [nameof(HiLoResult.High)] = cmd.OldMax + capacity,
                    [nameof(HiLoResult.LastSize)] = capacity,
                    [nameof(HiLoResult.ServerTag)] = RequestHandler.ServerStore.NodeTag,
                    [nameof(HiLoResult.LastRangeAt)] = DateTime.UtcNow.GetDefaultRavenFormat()
                });

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.AddStringToHttpContext(writer.ToString(), TrafficWatchChangeType.Hilo);
            }
        }
    }

    private static long CalculateCapacity(long lastSize, DateTime? lastRangeAt)
    {
        if (lastRangeAt == null)
            return Math.Max(32, lastSize);

        var span = DateTime.UtcNow - lastRangeAt.Value;

        if (span.TotalSeconds < 30)
        {
            return Math.Min(Math.Max(32, Math.Max(lastSize, lastSize * 2)), 1024 * 1024);
        }

        if (span.TotalSeconds > 60)
        {
            return Math.Max(32, lastSize / 2);
        }

        return Math.Max(32, lastSize);
    }

    internal sealed class MergedNextHiLoCommand : DocumentMergedTransactionCommand
    {
        public string Key;
        public DocumentDatabase Database;
        public long Capacity;
        public char Separator;
        public long LastRangeMax;
        public string Prefix;
        public long OldMax;

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var hiLoDocumentId = HiLoHandler.RavenHiloIdPrefix + Key;
            var prefix = Key + Separator;

            BlittableJsonReaderObject hiloDocReader = null;
            try
            {
                try
                {
                    hiloDocReader = Database.DocumentsStorage.Get(context, hiLoDocumentId)?.Data;
                }
                catch (DocumentConflictException e)
                {
                    throw new InvalidDataException("Failed to fetch HiLo document due to a conflict on the document. " +
                                                   "This shouldn't happen, since it this conflict should've been resolved during replication. " +
                                                   "This exception should not happen and is likely a bug.", e);
                }

                if (hiloDocReader == null)
                {
                    var newDoc = new DynamicJsonValue();

                    OldMax = LastRangeMax;
                    newDoc[nameof(HiloDocument.Max)] = OldMax + Capacity;
                    newDoc[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = CollectionName.HiLoCollection
                    };

                    using (var freshHilo = context.ReadObject(newDoc, hiLoDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                        Database.DocumentsStorage.Put(context, hiLoDocumentId, null, freshHilo);
                }
                else
                {
                    hiloDocReader.TryGet(nameof(HiloDocument.Max), out long oldMax);
                    OldMax = Math.Max(oldMax, LastRangeMax);

                    hiloDocReader.Modifications = new DynamicJsonValue(hiloDocReader)
                    {
                        [nameof(HiloDocument.Max)] = OldMax + Capacity
                    };

                    using (var freshHilo = context.ReadObject(hiloDocReader, hiLoDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                        Database.DocumentsStorage.Put(context, hiLoDocumentId, null, freshHilo);
                }

                Prefix = prefix;
            }
            finally
            {
                hiloDocReader?.Dispose();
            }
            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
        {
            return new MergedNextHiLoCommandDto
            {
                Key = Key,
                Capacity = Capacity,
                Separator = Separator,
                LastRangeMax = LastRangeMax,
                Prefix = Prefix,
                OldMax = OldMax
            };
        }
    }

    internal sealed class MergedNextHiLoCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedNextHiLoCommand>
    {
        public string Key;
        public long Capacity;
        public char Separator;
        public long LastRangeMax;
        public string Prefix;
        public long OldMax;

        public MergedNextHiLoCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new MergedNextHiLoCommand
            {
                Key = Key,
                Capacity = Capacity,
                Separator = Separator,
                LastRangeMax = LastRangeMax,
                Prefix = Prefix,
                OldMax = OldMax,
                Database = database
            };
        }
    }
}
