// -----------------------------------------------------------------------
//  <copyright file="HiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class HiLoHandler : DatabaseRequestHandler
    {
        private const string ravenKeyGeneratorsHilo = "Raven/Hilo/";
        private const string ravenKeyServerPrefix = "Raven/ServerPrefixForHilo";

        private static long CalculateCapacity(string lastSizeStr, string lastRangeAtStr)
        {
            long lastSize;
            DateTime lastRangeAt;
            if (long.TryParse(lastSizeStr, NumberStyles.Any, CultureInfo.InvariantCulture, out lastSize) == false ||
                DateTime.TryParseExact(lastRangeAtStr, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind,
                    out lastRangeAt) == false)
                return 32;

            var span = DateTime.UtcNow - lastRangeAt;

            if (span.TotalSeconds < 30)
            {
                return Math.Min(Math.Max(32,Math.Max(lastSize, lastSize * 2)), 1024 * 1024);
            }
            if (span.TotalSeconds > 60)
            {
                return Math.Max(lastSize / 2, 32);
            }

            return lastSize;
        }

        [RavenAction("/databases/*/hilo/next", "GET",
             "/databases/{databaseName:string}/hilo/next?tag={collectionName:string}&lastBatchSize={size:long|optional}&lastRangeAt={date:System.DateTime|optional}")]

        public async Task GetNextHiLo()
        {
            DocumentsOperationContext context;

            using (ContextPool.AllocateOperationContext(out context))
            {
                var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
                var lastSize = GetStringQueryString("lastBatchSize", false);
                var lastRangeAt = GetStringQueryString("lastRangeAt", false);

                var capacity = CalculateCapacity(lastSize, lastRangeAt);

                var cmd = new MergedNextHiLoCommand
                {
                    Database = Database,
                    Key = tag,
                    Capacity = capacity
                };

                await Database.TxMerger.Enqueue(cmd);

                HttpContext.Response.StatusCode = 201;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Prefix"] = cmd.HiLoResults.Prefix,
                        ["Low"] = cmd.HiLoResults.Low,
                        ["High"] = cmd.HiLoResults.High,

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
            public HiLoResults HiLoResults;

            public override void Execute(DocumentsOperationContext context, RavenTransaction tx)
            {

                //const string ravenKeyGeneratorsHilo = "Raven/Hilo/";
                //const string ravenKeyServerPrefix = "Raven/ServerPrefixForHilo";
                var hiLoDocumentKey = ravenKeyGeneratorsHilo + Key;
                string prefix = Key + "/";

                var document = Database.DocumentsStorage.Get(context, hiLoDocumentKey);
                var serverPrefixDoc = Database.DocumentsStorage.Get(context, ravenKeyServerPrefix);

                string serverPrefix;
                if (serverPrefixDoc != null && serverPrefixDoc.Data.TryGet("ServerPrefix", out serverPrefix))
                    prefix += serverPrefix;

                long oldMax = 0;
                var newDoc = new DynamicJsonValue();
                if (document != null)
                {
                    document.Data.TryGet("Max", out oldMax);
                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    for (int i = 0; i < document.Data.Count; i++)
                    {
                        document.Data.GetPropertyByIndex(0, ref prop);
                        if (prop.Name == "Max")
                            continue;
                        newDoc[prop.Name] = prop.Value;
                    }
                }

                newDoc["Max"] = oldMax + Capacity;

                using (var hiloReader = context.ReadObject(newDoc, hiLoDocumentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Database.DocumentsStorage.Put(context, hiLoDocumentKey, null, hiloReader);
                }
                HiLoResults = new HiLoResults(oldMax + 1, oldMax + Capacity, prefix);
            }
        }

        [RavenAction("/databases/*/hilo/return", "GET",
            "/databases/{databaseName:string}/hilo/return?tag={collectionName:string}end={lastGivenHigh:string}&last={lastIdUsed:string}")]
        public async Task HiLoReturn()
        {            
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var tag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("tag");
                var end = GetLongQueryString("end", required: true) ?? -1;
                var last = GetLongQueryString("last", required: true) ?? -1;

                var cmd = new MergedHiLoReturnCommand
                {
                    Database = Database,
                    Key = tag,
                    End = end,
                    Last = last
                };

                await Database.TxMerger.Enqueue(cmd);

                HttpContext.Response.StatusCode = 200;
            }
        }

        private class MergedHiLoReturnCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public DocumentDatabase Database;
            public long End;
            public long Last;

            public override void Execute(DocumentsOperationContext context, RavenTransaction tx)
            {
                //const string ravenKeyGeneratorsHilo = "Raven/Hilo/";
                var hiLoDocumentKey = ravenKeyGeneratorsHilo + Key;

                var document = Database.DocumentsStorage.Get(context, hiLoDocumentKey);

                if (document == null)
                    return;

                long oldMax;

                document.Data.TryGet("Max", out oldMax);

                if (oldMax != End || oldMax <= Last)
                    return;

                document.Data.Modifications = new DynamicJsonValue()
                {
                    ["Max"] = Last,
                };

                using (var freshHiLo = context.ReadObject(document.Data, hiLoDocumentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Database.DocumentsStorage.Put(context, hiLoDocumentKey, null, freshHiLo);
                }
            }
        }

    }
}
