using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21960 : RavenTestBase
{
    public RavenDB_21960(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public async Task CheckIndexesDebugMetadataEndpoint()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var o1 = new Order() { OrderedAt = new DateTime(2024, 1, 17, 15, 0, 0) };
                
                session.Store(o1);
                session.SaveChanges();

                var index = new DummyIndex();
                await index.ExecuteAsync(store);

                var otherIndex = new OtherDummyIndex();
                await otherIndex.ExecuteAsync(store);
                
                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var cmd = new IndexMetadataCommand();
                    await commands.ExecuteAsync(cmd);

                    var res = cmd.Result as BlittableJsonReaderObject;
                    
                    Assert.NotNull(res);
                    
                    res.TryGet("Results", out BlittableJsonReaderArray indexMetadataArray);
                    
                    Assert.Equal(2, indexMetadataArray.Length);

                    // First index

                    var firstIndex = (BlittableJsonReaderObject)indexMetadataArray[0];

                    Assert.Equal("DummyIndex", firstIndex["Name"].ToString());
                    Assert.Equal(IndexType.JavaScriptMap.ToString(), firstIndex["Type"].ToString());
                    Assert.Equal(IndexState.Normal.ToString(), firstIndex["State"].ToString());
                    Assert.Equal(IndexLockMode.Unlock.ToString(), firstIndex["LockMode"].ToString());
                    Assert.Equal(IndexSourceType.Documents.ToString(), firstIndex["SourceType"].ToString());
                    Assert.Equal(IndexPriority.Normal.ToString(), firstIndex["Priority"].ToString());
                    Assert.Equal(SearchEngineType.Lucene.ToString(), firstIndex["SearchEngineType"].ToString());
                    Assert.Equal(false, firstIndex["HasDynamicFields"]);
                    Assert.Equal(false, firstIndex["HasCompareExchange"]);
                    Assert.Equal(true, firstIndex["HasTimeFields"]);
                    Assert.Equal("OrderedAt", ((BlittableJsonReaderArray)firstIndex["TimeFields"])[0].ToString());

                    var db = await GetDatabase(store.Database);
                    
                    var indexInstance = db.IndexStore.GetIndex(index.IndexName);

                    Assert.Equal(indexInstance.Definition.Version, firstIndex["Version"]);

                    //// Second index

                    var secondIndex = (BlittableJsonReaderObject)indexMetadataArray[1];

                    Assert.Equal("OtherDummyIndex", secondIndex["Name"].ToString());
                    Assert.Equal(IndexType.Map.ToString(), secondIndex["Type"].ToString());
                    Assert.Equal(IndexState.Normal.ToString(), secondIndex["State"].ToString());
                    Assert.Equal(IndexLockMode.Unlock.ToString(), secondIndex["LockMode"].ToString());
                    Assert.Equal(IndexSourceType.Documents.ToString(), secondIndex["SourceType"].ToString());
                    Assert.Equal(IndexPriority.Normal.ToString(), secondIndex["Priority"].ToString());
                    Assert.Equal(SearchEngineType.Corax.ToString(), secondIndex["SearchEngineType"].ToString());
                    Assert.Equal(false, secondIndex["HasDynamicFields"]);
                    Assert.Equal(false, secondIndex["HasCompareExchange"]);
                    Assert.Equal(false, secondIndex["HasTimeFields"]);
                    Assert.Equal(0, ((BlittableJsonReaderArray)secondIndex["TimeFields"]).Length);

                    var otherIndexInstance = db.IndexStore.GetIndex(otherIndex.IndexName);

                    Assert.Equal(otherIndexInstance.Definition.Version, secondIndex["Version"]);
                }
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task CheckIndexesDebugMetadataEndpointWithNameParameter()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var o1 = new Order() { OrderedAt = new DateTime(2024, 1, 17, 15, 0, 0) };
                
                session.Store(o1);
                session.SaveChanges();

                var index = new DummyIndex();
                await index.ExecuteAsync(store);

                var otherIndex = new OtherDummyIndex();
                await otherIndex.ExecuteAsync(store);
                
                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var cmd = new IndexMetadataCommand(otherIndex.IndexName);
                    await commands.ExecuteAsync(cmd);

                    var res = cmd.Result as BlittableJsonReaderObject;
                    
                    Assert.NotNull(res);
                    
                    res.TryGet("Results", out BlittableJsonReaderArray indexMetadataArray);

                    Assert.Equal(1, indexMetadataArray.Length);

                    var metadata = (BlittableJsonReaderObject)indexMetadataArray[0];

                    Assert.Equal("OtherDummyIndex", metadata["Name"].ToString());
                    Assert.Equal(IndexType.Map.ToString(), metadata["Type"].ToString());
                    Assert.Equal(IndexState.Normal.ToString(), metadata["State"].ToString());
                    Assert.Equal(IndexLockMode.Unlock.ToString(), metadata["LockMode"].ToString());
                    Assert.Equal(IndexSourceType.Documents.ToString(), metadata["SourceType"].ToString());
                    Assert.Equal(IndexPriority.Normal.ToString(), metadata["Priority"].ToString());
                    Assert.Equal(SearchEngineType.Corax.ToString(), metadata["SearchEngineType"].ToString());
                    Assert.Equal(false, metadata["HasDynamicFields"]);
                    Assert.Equal(false, metadata["HasCompareExchange"]);
                    Assert.Equal(false, metadata["HasTimeFields"]);
                    Assert.Equal(0, ((BlittableJsonReaderArray)metadata["TimeFields"]).Length);

                    var db = await GetDatabase(store.Database);

                    var otherIndexInstance = db.IndexStore.GetIndex(otherIndex.IndexName);

                    Assert.Equal(otherIndexInstance.Definition.Version, metadata["Version"]);
                }
            }
        }
    }

    private class Order
    {
        public DateTime OrderedAt { get; set; }
    }
    
    private class DummyIndex : AbstractJavaScriptIndexCreationTask
    {
        public DummyIndex()
        {
            Maps = new HashSet<string>
            {
                @"map('Orders', function (order){ 
                    return { 
                        OrderedAt: order.OrderedAt
                    };
                })",
            };
        }
    }
    
    private class OtherDummyIndex : AbstractIndexCreationTask<Order>
    {
        public OtherDummyIndex()
        {
            Map = orders => from order in orders
                select new
                {
                    Whatever = "abc"
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class IndexMetadataCommand : RavenCommand<object>
    {
        private readonly string _indexName;
        public IndexMetadataCommand(string indexName = null)
        {
            _indexName = indexName;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (_indexName == null)
                url = $"{node.Url}/databases/{node.Database}/indexes/debug/metadata";
            else
                url = $"{node.Url}/databases/{node.Database}/indexes/debug/metadata?name={_indexName}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = response;
        }

        public override bool IsReadRequest => true;
    }
}
