using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class MapReduceProjection : RavenTestBase
{
    public MapReduceProjection(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task ProjectionWillReturnCorrectTypeFromOriginalReduceMap(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new MapReduceDtoIndex();
        await index.ExecuteAsync(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Dto(){Name = "Maciej"});
            session.SaveChanges();
        }
        
        Indexes.WaitForIndexing(store);
        
        using (var commands = store.Commands())
        {
            var command = new QueryCommand(commands.Session, new IndexQuery
            {
                Query = $"from index '{index.IndexName}' select Count"
            });

            await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

            var results = new DynamicArray(command.Result.Results)[0]["Count"];
            
            Assert.False(results is LazyStringValue or string);
            Assert.True(results is long);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class MapReduceDtoIndex : AbstractIndexCreationTask<Dto, MapReduceDtoIndex.ReduceResult>
    {
        public class ReduceResult
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public MapReduceDtoIndex()
        {
            Map = dtos => dtos.Select(i => new {Name = i.Name, Count = 1});
            Reduce = results => from reduce in results
                group reduce by reduce.Name
                into g
                select new ReduceResult() {Name = g.Key, Count = g.Sum(i => i.Count)};
        }
    }
}
