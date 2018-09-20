// -----------------------------------------------------------------------
//  <copyright file="HiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class HiLoHandler : DatabaseRequestHandler
    {
        public const string RavenHiloIdPrefix = "Raven/Hilo/";

        private static long CalculateCapacity(long lastSize, string lastRangeAtStr)
        {
            if (DateTime.TryParseExact(lastRangeAtStr, DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime lastRangeAt) == false)
                return Math.Max(32, lastSize);

            var span = DateTime.UtcNow - lastRangeAt;

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

        [RavenAction("/databases/*/hilo/next", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetNextHiLo()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
                var lastSize = GetLongQueryString("lastBatchSize", false) ?? 0;
                var lastRangeAt = GetStringQueryString("lastRangeAt", false);
                var identityPartsSeparator = GetStringQueryString("identityPartsSeparator", false) ?? "/";
                var lastMax = GetLongQueryString("lastMax", false) ?? 0;

                var capacity = CalculateCapacity(lastSize, lastRangeAt);

                var cmd = new MergedNextHiLoCommand
                {
                    Database = Database,
                    Key = tag,
                    Capacity = capacity,
                    Separator = identityPartsSeparator,
                    LastRangeMax = lastMax
                };

                await Database.TxMerger.Enqueue(cmd);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Prefix"] = cmd.Prefix,
                        ["Low"] = cmd.OldMax + 1,
                        ["High"] = cmd.OldMax + capacity,
                        ["LastSize"] = capacity,
                        ["ServerTag"] = ServerStore.NodeTag,
                        ["LastRangeAt"] = DateTime.UtcNow.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)
                    });
                    if (TrafficWatchManager.HasRegisteredClients)
                        AddStringToHttpContext(writer.ToString(), TrafficWatchChangeType.Hilo);
                }
            }
        }

        internal class MergedNextHiLoCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public DocumentDatabase Database;
            public long Capacity;
            public string Separator;
            public long LastRangeMax;
            public string Prefix;
            public long OldMax;

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var hiLoDocumentId = RavenHiloIdPrefix + Key;
                var prefix = Key + Separator;

                var newDoc = new DynamicJsonValue();
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
                        OldMax = LastRangeMax;
                        newDoc["Max"] = OldMax + Capacity;
                        newDoc[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = CollectionName.HiLoCollection
                        };

                        using (var freshHilo = context.ReadObject(newDoc, hiLoDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                            Database.DocumentsStorage.Put(context, hiLoDocumentId, null, freshHilo);
                    }
                    else
                    {

                        hiloDocReader.TryGet("Max", out long oldMax);
                        OldMax = Math.Max(oldMax, LastRangeMax);

                        hiloDocReader.Modifications = new DynamicJsonValue(hiloDocReader)
                        {
                            ["Max"] = OldMax + Capacity
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

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
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

        [RavenAction("/databases/*/hilo/return", "PUT", AuthorizationStatus.ValidUser)]
        public async Task HiLoReturn()
        {
            var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
            var end = GetLongQueryString("end");
            var last = GetLongQueryString("last");

            var cmd = new MergedHiLoReturnCommand
            {
                Database = Database,
                Key = tag,
                End = end,
                Last = last
            };

            await Database.TxMerger.Enqueue(cmd);

            NoContentStatus();
        }

        internal class MergedHiLoReturnCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public DocumentDatabase Database;
            public long End;
            public long Last;

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var hiLoDocumentId = RavenHiloIdPrefix + Key;

                var document = Database.DocumentsStorage.Get(context, hiLoDocumentId);

                if (document == null)
                    return 1;

                document.Data.TryGet("Max", out long oldMax);
                if (oldMax != End || Last > oldMax)
                    return 1;

                document.Data.Modifications = new DynamicJsonValue
                {
                    ["Max"] = Last
                };

                using (var hiloReader = context.ReadObject(document.Data, hiLoDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Database.DocumentsStorage.Put(context, hiLoDocumentId, null, hiloReader);
                }

                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedHiLoReturnCommandDto
                {
                    Key = Key,
                    End = End,
                    Last = Last
                };
            }
        }

    }

    internal class MergedHiLoReturnCommandDto : TransactionOperationsMerger.IReplayableCommandDto<HiLoHandler.MergedHiLoReturnCommand>
    {
        public string Key;
        public long End;
        public long Last;

        public HiLoHandler.MergedHiLoReturnCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new HiLoHandler.MergedHiLoReturnCommand()
            {
                Key = Key,
                End = End,
                Last = Last,
                Database = database
            };
        }
    }

    internal class MergedNextHiLoCommandDto : TransactionOperationsMerger.IReplayableCommandDto<HiLoHandler.MergedNextHiLoCommand>
    {
        public string Key;
        public long Capacity;
        public string Separator;
        public long LastRangeMax;
        public string Prefix;
        public long OldMax;

        public HiLoHandler.MergedNextHiLoCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new HiLoHandler.MergedNextHiLoCommand
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
