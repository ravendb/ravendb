// -----------------------------------------------------------------------
//  <copyright file="HiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class HiLoHandler : DatabaseRequestHandler
    {
        private const string RavenKeyGeneratorsHilo = "Raven/Hilo/";
        private const string RavenKeyServerPrefix = "Raven/ServerPrefixForHilo";

        private static long CalculateCapacity(long lastSize, string lastRangeAtStr)
        {
            DateTime lastRangeAt;
            if (DateTime.TryParseExact(lastRangeAtStr, "o", CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out lastRangeAt) == false)
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

        [RavenAction("/databases/*/hilo/next", "GET", "/databases/{databaseName:string}/hilo/next?tag={collectionName:string}&lastBatchSize={size:long|optional}&lastRangeAt={date:System.DateTime|optional}&identityPartsSeparator={separator:string|optional}&lastMax={max:long|optional} ")]
        public async Task GetNextHiLo()
        {
            DocumentsOperationContext context;

            using (ContextPool.AllocateOperationContext(out context))
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
                        ["LastRangeAt"] = DateTime.UtcNow.ToString("o")
                    });
                }
            }
        }

        private class MergedNextHiLoCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public DocumentDatabase Database;
            public long Capacity;
            public string Separator;
            public long LastRangeMax;
            public string Prefix;
            public long OldMax;

            public override int Execute(DocumentsOperationContext context)
            {
                var hiLoDocumentKey = RavenKeyGeneratorsHilo + Key;
                var prefix = Key + Separator;

                long oldMax = 0;
                var newDoc = new DynamicJsonValue();
                BlittableJsonReaderObject hiloDocReader = null, serverPrefixDocReader = null;
                try
                {
                    try
                    {
                        serverPrefixDocReader = Database.DocumentsStorage.Get(context, RavenKeyServerPrefix)?.Data;
                        hiloDocReader = Database.DocumentsStorage.Get(context, hiLoDocumentKey)?.Data;
                    }
                    catch (DocumentConflictException e)
                    {
                        throw new InvalidDataException(@"Failed to fetch HiLo document due to a conflict 
                                                            on the document. This shouldn't happen, since
                                                            it this conflict should've been resolved during replication.
                                                             This exception should not happen and is likely a bug.", e);
                    }

                    string serverPrefix;
                    if (serverPrefixDocReader != null &&
                        serverPrefixDocReader.TryGet("ServerPrefix", out serverPrefix))
                        prefix += serverPrefix;

                    if (hiloDocReader != null)
                    {
                        var prop = new BlittableJsonReaderObject.PropertyDetails();
                        foreach (var propertyId in hiloDocReader.GetPropertiesByInsertionOrder())
                        {
                            hiloDocReader.GetPropertyByIndex(propertyId, ref prop);
                            if (prop.Name == "Max")
                            {
                                oldMax = (long)prop.Value;
                                continue;
                            }

                            newDoc[prop.Name] = prop.Value;
                        }
                    }

                    oldMax = Math.Max(oldMax, LastRangeMax);

                    newDoc["Max"] = oldMax + Capacity;

                    using (var freshHilo = context.ReadObject(newDoc, hiLoDocumentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                        Database.DocumentsStorage.Put(context, hiLoDocumentKey, null, freshHilo);

                    OldMax = oldMax;
                    Prefix = prefix;
                }
                finally
                {
                    serverPrefixDocReader?.Dispose();
                    hiloDocReader?.Dispose();
                }
                return 1;
            }
        }

        [RavenAction("/databases/*/hilo/return", "PUT", "/databases/{databaseName:string}/hilo/return?tag={collectionName:string}&end={lastGivenHigh:string}&last={lastIdUsed:string}")]
        public async Task HiLoReturn()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
                var end = GetLongQueryString("end").Value;
                var last = GetLongQueryString("last").Value;

                var cmd = new MergedHiLoReturnCommand
                {
                    Database = Database,
                    Key = tag,
                    End = end,
                    Last = last
                };

                await Database.TxMerger.Enqueue(cmd);
            }

            NoContentStatus();
        }

        private class MergedHiLoReturnCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public DocumentDatabase Database;
            public long End;
            public long Last;

            public override int Execute(DocumentsOperationContext context)
            {
                var hiLoDocumentKey = RavenKeyGeneratorsHilo + Key;

                var document = Database.DocumentsStorage.Get(context, hiLoDocumentKey);

                if (document == null)
                    return 1;

                long oldMax;

                document.Data.TryGet("Max", out oldMax);

                if (oldMax != End || Last > oldMax)
                    return 1;

                document.Data.Modifications = new DynamicJsonValue()
                {
                    ["Max"] = Last,
                };

                using (var hiloReader = context.ReadObject(document.Data, hiLoDocumentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Database.DocumentsStorage.Put(context, hiLoDocumentKey, null, hiloReader);
                }

                return 1;
            }
        }

    }
}