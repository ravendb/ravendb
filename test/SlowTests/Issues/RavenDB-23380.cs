using System.Collections.Generic;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Indexes.Test;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23380 : RavenTestBase
{
    public RavenDB_23380(ITestOutputHelper output) : base(output)
    {
    }
    
    private class PutTestIndexCommand : RavenCommand<object>
    {
        private readonly TestIndexParameters _payload;
        private readonly int _shardNumber;
        private readonly bool _isSharded;
        
        public PutTestIndexCommand(TestIndexParameters payload, bool isSharded = false, int shardNumber = 0)
        {
            _payload = payload;
            _isSharded = isSharded;
            _shardNumber = shardNumber;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (_isSharded)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/test?nodeTag={node.ClusterTag}&shardNumber={_shardNumber}";
            }
            else
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/test";
            }

            var payloadJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_payload, ctx);

            var documentConventions = new DocumentConventions();

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, payloadJson);
                }, documentConventions)
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

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void IncorrectAnalyzerNameShouldThrowSharded(Options options) => IncorrectAnalyzerNameShouldThrow(options, isSharded: true);
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void IncorrectAnalyzerNameShouldThrowSingle(Options options) => IncorrectAnalyzerNameShouldThrow(options, isSharded: false);
    
    private void IncorrectAnalyzerNameShouldThrow(Options options, bool isSharded)
    {
        var payload = new TestIndexParameters()
        {
            IndexDefinition = new IndexDefinition() { Maps = new HashSet<string> { "from dto in docs.Dtos select new { Name = dto.Name }" }, Fields = new Dictionary<string, IndexFieldOptions> { { "Name", new IndexFieldOptions { Analyzer = "NonExistingAnalyzer", Indexing = FieldIndexing.Search } } } },
            Query = "from index '<TestIndexName>' select Name"
        };
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1" };
                var dto2 = new Dto() { Name = "Name2" };

                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = isSharded ? new PutTestIndexCommand(payload, isSharded: true, shardNumber: 0) : new PutTestIndexCommand(payload);

                var ex = Assert.Throws<IndexCompilationException>(() => commands.Execute(cmd));
                
                Assert.Contains("Cannot find analyzer type 'NonExistingAnalyzer' for field: Name", ex.Message);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void InvalidDefinitionShouldThrowSharded(Options options) => InvalidDefinitionShouldThrow(options, isSharded: true);
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void InvalidDefinitionShouldThrowSingle(Options options) => InvalidDefinitionShouldThrow(options, isSharded: false);
    
    private void InvalidDefinitionShouldThrow(Options options, bool isSharded)
    {
        var payload = new TestIndexParameters()
        {
            IndexDefinition = new IndexDefinition() { Maps = new HashSet<string> { "from dto in docs.Dtos select new { Value = dto.Value / (dto.Value - dto.Value) }" } },
            Query = "from index '<TestIndexName>' select Value"
        };
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Value = 21 };
                session.Store(dto1);
                session.SaveChanges();
                
                using (var commands = store.Commands())
                {
                    PutTestIndexCommand cmd;
                    
                    if (isSharded)
                    {
                        var shardNumber = Sharding.GetShardNumberForAsync(store, dto1.Id).GetAwaiter().GetResult();
                        cmd = new PutTestIndexCommand(payload, isSharded: true, shardNumber: shardNumber);
                    }
                    else
                        cmd = new PutTestIndexCommand(payload);

                    Assert.Throws<RavenException>(() => commands.Execute(cmd));
                }
            }
        }
    }
    
    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
    }
}
