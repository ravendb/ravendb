using System;
using System.Linq;
using FastTests;
using Lucene.Net.Analysis.Standard;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21573 : RavenTestBase
{
    public RavenDB_21573(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void WhenFieldHasDatesWeStillCanUseAnalyzerToFindOtherValues(Options options)
    {
        using var store = GetDocumentStore(options);
        new DtoIndex().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("MyMessageType", null, new object[] { TimeSpan.FromSeconds(5), "SomeEndpoint Send MyMessageType" }));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);
        int count = session.Query<Dto, DtoIndex>().Search(x => x.Name, "MyMessageType").Count();
        Assert.Equal(1, count);

        count = session.Query<Dto, DtoIndex>().Count(x => x.Name == "MyMessageType");
        Assert.Equal(1, count);
    }

    private record Dto(string Name, string Id, object[] OtherValues);

    private class DtoIndex : AbstractIndexCreationTask<Dto>
    {
        public DtoIndex()
        {
            Map = dtos =>
                from dto in dtos
                select new {Name = new string[] {dto.Name}.Union(dto.OtherValues), dto.Id};
            Index(x => x.Name, FieldIndexing.Search);
            Analyze(x => x.Name, typeof(StandardAnalyzer).AssemblyQualifiedName);
        }
    }
}
