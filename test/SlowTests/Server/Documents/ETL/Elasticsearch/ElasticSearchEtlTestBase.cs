using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Elastic.Clients.Elasticsearch;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.ElasticSearch
{
    public class ElasticSearchEtlTestBase : RavenTestBase
    {
        private HashSet<string> _definedIndexes = new HashSet<string>();

        public ElasticSearchEtlTestBase(ITestOutputHelper output) : base(output)
        {
            IndexSuffix = Guid.NewGuid().ToString().Replace("-", string.Empty);
        }

        protected string IndexSuffix { get; set; }

        protected string[] DefaultCollections = { "Orders" };

        protected string OrdersIndexName => $"Orders{IndexSuffix}".ToLower();

        protected string OrderLinesIndexName => $"OrderLines{IndexSuffix}".ToLower();

        protected string DefaultScript =>
            @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    var cost = (line.Quantity * line.PricePerUnit) *  ( 1 - line.Discount);
    orderData.TotalCost += line.Cost * line.Quantity;
    loadToOrderLines" + IndexSuffix + @"({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.Cost
    });
}

loadToOrders" + IndexSuffix + @"(orderData);";

        protected List<ElasticSearchIndex> DefaultIndexes => new List<ElasticSearchIndex>()
        {
            new() {IndexName = OrdersIndexName, DocumentIdProperty = "Id"},
            new() {IndexName = OrderLinesIndexName, DocumentIdProperty = "OrderId"},
        };

        protected ElasticSearchEtlConfiguration SetupElasticEtl(DocumentStore store, string script, IEnumerable<ElasticSearchIndex> indexes, IEnumerable<string> collections, bool applyToAllDocuments = false,
            global::Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication authentication = null, string configurationName = null, string transformationName = null, string[] nodes = null)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to ELASTIC";

            foreach (var index in indexes)
            {
                _definedIndexes.Add(index.IndexName);
            }

            var config = new ElasticSearchEtlConfiguration
            {
                Name = configurationName ?? connectionStringName,
                ConnectionStringName = connectionStringName,
                ElasticIndexes = indexes.ToList(),
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName ?? $"ETL : {connectionStringName}",
                        Collections = new List<string>(collections),
                        Script = script,
                        ApplyToAllDocuments = applyToAllDocuments
                    }
                },
            };

            Etl.AddEtl(store, config,
                new ElasticSearchConnectionString
                {
                    Name = connectionStringName,
                    Nodes = nodes ?? ElasticSearchTestNodes.Instance.VerifiedNodes.Value,
                    Authentication = authentication,
                });

            return config;
        }

        protected IDisposable GetElasticClient(out ElasticsearchClient client)
        {
            ElasticsearchClient localClient = client =
                ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString {Nodes = ElasticSearchTestNodes.Instance.VerifiedNodes.Value},
                    useCustomBlittableSerializer: false);
            
            AsyncHelpers.RunSync(() => CleanupIndexes(localClient));

            return new DisposableAction(() =>
            {
                AsyncHelpers.RunSync(() => CleanupIndexes(localClient));
            });
        }

        private async Task CleanupIndexes(ElasticsearchClient client)
        {
            if (_definedIndexes.Count <= 0)
                return;

            foreach (var indexName in _definedIndexes)
            {
                var response = await client.Indices.DeleteAsync(Indices.Index(indexName.ToLower()));

                if (response.IsValidResponse)
                    continue;

                if (response.ElasticsearchServerError?.Status == 404)
                    return;

                Exception inner;

                response.TryGetOriginalException(out var exception);
                if (Context.TestException != null)
                    inner = new AggregateException(exception, Context.TestException);
                else
                    inner = exception;

                throw new InvalidOperationException($"Failed to cleanup indexes: {response.ElasticsearchServerError}. Check inner exceptions for details", inner);
            }
        }

        protected async Task AssertEtlDoneAsync(ManualResetEventSlim etlDone, TimeSpan timeout, string databaseName, ElasticSearchEtlConfiguration config)
        {
            if (etlDone.Wait(timeout) == false)
            {
                var loadError = await Etl.TryGetLoadErrorAsync(databaseName, config);
                var transformationError = await Etl.TryGetTransformationErrorAsync(databaseName, config);

                Assert.Fail($"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
            }
        }
    }
}
