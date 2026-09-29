using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_27541 : RavenTestBase
    {
        public RavenDB_27541(ITestOutputHelper output) : base(output)
        {
        }

        private const int Documents = 20;
        private const int MarketsPerDocument = 150;
        private const int OutcomesPerMarket = 2;

        /// <summary>
        /// All documents of a patch-by-query batch are written back through the same DocumentsOperationContext.
        /// The write-back must start a new document in the context's CachedProperties, otherwise every document
        /// written after the first one embeds the property names of the documents written before it.
        /// With id-keyed maps (hundreds of unique names per document) this grew the stored documents by ~23%
        /// after a patch that changed a single root property.
        /// </summary>
        [RavenFact(RavenTestCategory.Patching)]
        public async Task PatchingManyDocumentsWithIdKeyedMapsInOneTransactionMustNotGrowTheStoredDocuments()
        {
            using var store = GetDocumentStore();
            var database = await GetDocumentDatabaseInstanceFor(store);
            var requestExecutor = store.GetRequestExecutor();

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                for (int i = 0; i < Documents; i++)
                {
                    var doc = ctx.ReadObject(CreateMapShapedDocument(i, withMetadata: true), "events/" + i);
                    requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "events/" + i, null, doc), ctx);
                }
            }

            var sizesBefore = GetStoredSizes(database);

            var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from Events update { this.Touched = true; }"));
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var sizesAfter = GetStoredSizes(database);

            var failures = new List<string>();
            for (int i = 0; i < Documents; i++)
            {
                var id = "events/" + i;
                var growth = (double)sizesAfter[id] / sizesBefore[id];
                Output.WriteLine($"{id}: {sizesBefore[id]:N0} -> {sizesAfter[id]:N0} bytes ({growth:F2}x)");

                // 'Touched' adds a few bytes; anything beyond that is property names that belong to other documents
                if (growth > 1.05)
                    failures.Add($"{id}: {sizesBefore[id]:N0} -> {sizesAfter[id]:N0} bytes ({growth:F2}x)");
            }

            Assert.True(failures.Count == 0,
                "Stored documents grew after a patch that set one boolean, because the write-back embedded the property names of the other documents in the batch:" +
                Environment.NewLine + string.Join(Environment.NewLine, failures));
        }

        /// <summary>
        /// Rebuilding a document through ManualBlittableJsonDocumentBuilder (the patch write-back path) registers
        /// every new property name in the context's CachedProperties. Every object close that followed a new name
        /// then renumbered the global sort order of *all* names the context had seen, so the cost of writing one
        /// document grew with the number of names accumulated from the documents written before it in the same
        /// context: ~10 ms for the first document, ~130 ms by the tenth, seconds later on. The same bytes shaped
        /// as arrays of objects with a Key field are not affected.
        /// </summary>
        [RavenFact(RavenTestCategory.Core)]
        public void RebuildingDocumentsWithIdKeyedMapsMustNotGetSlowerAsTheContextSeesMorePropertyNames()
        {
            const int documents = 30;

            // each source document lives in its own context so that parsing them does not accumulate names
            var sourceContexts = new List<JsonOperationContext>();
            var sources = new List<BlittableJsonReaderObject>();
            try
            {
                for (int i = 0; i < documents; i++)
                {
                    var ctx = JsonOperationContext.ShortTermSingleUse();
                    sourceContexts.Add(ctx);
                    sources.Add(ctx.ReadObject(CreateMapShapedDocument(i, withMetadata: false), "events/" + i));
                }

                using (var warmup = JsonOperationContext.ShortTermSingleUse())
                {
                    for (int i = 0; i < 3; i++)
                        Rebuild(warmup, sources[0]).Dispose();
                }

                // one context for all documents, like the merger's context across the commands of a merged transaction
                var timings = new double[documents];
                using (var writeContext = JsonOperationContext.ShortTermSingleUse())
                {
                    for (int i = 0; i < documents; i++)
                    {
                        var sw = Stopwatch.StartNew();
                        Rebuild(writeContext, sources[i]).Dispose();
                        timings[i] = sw.Elapsed.TotalMilliseconds;
                    }
                }

                var first = Median(timings.Take(5));
                var last = Median(timings.Skip(documents - 5));
                var ratio = last / first;
                Output.WriteLine($"first documents {first:F2} ms, last documents {last:F2} ms, ratio {ratio:F1}x");

                // before the fix the last documents were ~10x slower than the first ones on the same hardware
                Assert.True(ratio < 4, $"Writing the same-sized document became {ratio:F1}x slower ({first:F2} ms -> {last:F2} ms) as the context accumulated distinct property names.");
            }
            finally
            {
                foreach (var ctx in sourceContexts)
                    ctx.Dispose();
            }
        }

        // same path as the untouched parts of a patched document in JsBlittableBridge.WriteBlittableInstance:
        // ManualBlittableJsonDocumentBuilder.WriteValue(StartObject) -> BlittableJsonReaderObject.AddItemsToStream
        private static BlittableJsonReaderObject Rebuild(JsonOperationContext context, BlittableJsonReaderObject source)
        {
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                context.CachedProperties.NewDocument();
                builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                builder.StartWriteObjectDocument();
                builder.StartWriteObject();
                source.AddItemsToStream(builder);
                builder.WriteObjectEnd();
                builder.FinalizeDocument();
                return builder.CreateReader();
            }
        }

        private static double Median(IEnumerable<double> values)
        {
            var sorted = values.OrderBy(x => x).ToList();
            return sorted[sorted.Count / 2];
        }

        private static Dictionary<string, int> GetStoredSizes(DocumentDatabase database)
        {
            var sizes = new Dictionary<string, int>();
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                for (int i = 0; i < Documents; i++)
                {
                    var id = "events/" + i;
                    using var document = database.DocumentsStorage.Get(context, id);
                    sizes[id] = document.Data.Size;
                }
            }

            return sizes;
        }

        // { Markets: { "evt:7:0": { Id, DisplayName, State, Outcomes: { "evt:7:0:0": {...}, "evt:7:0:1": {...} } }, ... } }
        private static DynamicJsonValue CreateMapShapedDocument(int docId, bool withMetadata)
        {
            var markets = new DynamicJsonValue();
            for (int m = 0; m < MarketsPerDocument; m++)
            {
                var marketId = $"evt:{docId}:{m}";
                var outcomes = new DynamicJsonValue();
                for (int o = 0; o < OutcomesPerMarket; o++)
                    outcomes[$"{marketId}:{o}"] = CreateOutcome($"{marketId}:{o}");

                markets[marketId] = CreateMarket(marketId, outcomes);
            }

            var document = new DynamicJsonValue
            {
                ["Name"] = "event " + docId,
                ["State"] = "Live",
                ["Markets"] = markets
            };

            if (withMetadata)
                document["@metadata"] = new DynamicJsonValue { ["@collection"] = "Events" };

            return document;
        }

        private static DynamicJsonValue CreateMarket(string id, object outcomes) => new()
        {
            ["Id"] = id,
            ["DisplayName"] = "Market " + id,
            ["State"] = "Open",
            ["TradingState"] = 1,
            ["Outcomes"] = outcomes
        };

        private static DynamicJsonValue CreateOutcome(string id) => new()
        {
            ["Id"] = id,
            ["DisplayName"] = "Outcome " + id,
            ["Tip"] = 1.85,
            ["IsVisible"] = true
        };
    }
}
