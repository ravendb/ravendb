using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22537 : RavenTestBase
{
    public RavenDB_22537(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestInQuery(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = null };
                var dto2 = new Dto() { Name = null };
                var dto3 = new Dto() { Name = "NotNullName" };
                 
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);
                
                session.SaveChanges();
                
                // One term
                var names = new List<string>() { null };
                
                var res = session.Query<Dto>().Where(x => x.Name.In(names)).ToList();

                Assert.Equal(2, res.Count);

                // Two terms
                names = new List<string>() { null, "NotNullName" };
                
                res = session.Query<Dto>().Where(x => x.Name.In(names)).ToList();

                Assert.Equal(3, res.Count);
                
                // Four terms
                names = new List<string>() { null, "Something1", "Something2", "Something3" };
                
                res = session.Query<Dto>().Where(x => x.Name.In(names)).ToList();

                Assert.Equal(2, res.Count);
                
                names = new List<string>() { null, "NotNullName", "Something1", "Something2" };
                
                res = session.Query<Dto>().Where(x => x.Name.In(names)).ToList();

                Assert.Equal(3, res.Count);
                
                // Five terms
                names = new List<string>() { null, "NotNullName", "Something1", "Something2", "Something3" };
                
                res = session.Query<Dto>().Where(x => x.Name.In(names)).ToList();

                Assert.Equal(3, res.Count);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestAllInQuery(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = null, Names = new List<string>() { null } };
                var dto2 = new Dto() { Name = null, Names = new List<string>() { null } };
                var dto3 = new Dto() { Name = "NotNullName", Names = new List<string>() { "NotNullName" } };
                 
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);
                
                session.SaveChanges();
                
                // One term
                var names = new List<string>() { null };

                var res = session.Query<Dto>().Where(x => x.Names.ContainsAll(names)).ToList();
                
                Assert.Equal(2, res.Count);
                
                names = new List<string>() { "NotNullName" };
                
                res = session.Query<Dto>().Where(x => x.Names.ContainsAll(names)).ToList();
                
                Assert.Equal(1, res.Count);
                
                // Two terms
                names = new List<string>() { null, "NotNullName" };
                
                var dto4 = new Dto() { Name = null, Names = names };
                
                session.Store(dto4);
                
                session.SaveChanges();
                
                Indexes.WaitForIndexing(store);
                
                res = session.Query<Dto>().Where(x => x.Names.ContainsAll(names)).ToList();
                
                Assert.Equal(1, res.Count);
                
                // Five terms
                names = new List<string>() { null, "NotNullName", "Something1", "Something2", "Something3" };
                
                var dto5 = new Dto() { Name = null, Names = names };
                
                session.Store(dto5);
                
                session.SaveChanges();
                
                Indexes.WaitForIndexing(store);
                
                res = session.Query<Dto>().Where(x => x.Names.ContainsAll(names)).ToList();
                
                Assert.Equal(1, res.Count);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        public List<string> Names { get; set; }
    }
}
