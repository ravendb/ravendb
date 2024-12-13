using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23076 : RavenTestBase
{
    public RavenDB_23076(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.None)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestRavenVectorStorageAndLoad(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var vectorToStore = new RavenVector<float>(new[] { 0.1f, 0.2f });
            
            using (var session = store.OpenSession())
            {
                var dto = new Dto()
                {
                    Id = "Dtos/1",
                    Vector = vectorToStore
                };
                
                session.Store(dto);
                
                session.SaveChanges();

                var index = new IndexWithServerSideType();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var entries = session.Query<IndexWithServerSideType.IndexEntry, IndexWithServerSideType>().Where(x => x.UnderlyingArrayType == "Sparrow.Json.BlittableJsonReaderVector").ProjectInto<Dto>().ToList();

                Assert.Equal(1, entries.Count);
            }
            
            using (var session = store.OpenSession())
            {
                var loadedDto = session.Load<Dto>("Dtos/1");

                Assert.Equal(loadedDto.Vector, vectorToStore);
            }
        }
    }

    public class Dto
    {
        public string Id { get; set; }
        public RavenVector<float> Vector { get; set; }
    }

    public class IndexWithServerSideType : AbstractIndexCreationTask<Dto>
    {
        public class IndexEntry
        {
            public string UnderlyingArrayType { get; set; }
        }
        
        public IndexWithServerSideType()
        {
            Map = dtos => from dto in dtos
                select new IndexEntry() { UnderlyingArrayType = ((Raven.Server.Documents.Indexes.Static.DynamicBlittableJson)(object)dto.Vector)["@vector"].GetType().FullName };
        }
    }
}
