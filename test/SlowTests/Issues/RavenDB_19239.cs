using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19239 : RavenTestBase
{
    public RavenDB_19239(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestLockedError()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1" };
                var dto2 = new Dto() { Name = "Name2" };

                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
                
                var index = new DummyIndex();

                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                store.Maintenance.Send(new SetIndexesLockOperation("DummyIndex", IndexLockMode.LockedError));
                
                var ex = Assert.Throws<RavenException>(() => store.Maintenance.Send(new DeleteIndexOperation("DummyIndex")));

                Assert.Contains("Cannot delete existing index DummyIndex with lock mode LockedError", ex.ToString());
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestLockedIgnore()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1" };
                var dto2 = new Dto() { Name = "Name2" };

                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
                
                var index = new DummyIndex();

                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                store.Maintenance.Send(new SetIndexesLockOperation("DummyIndex", IndexLockMode.LockedIgnore));
                
                store.Maintenance.Send(new DeleteIndexOperation("DummyIndex"));
                
                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("DummyIndex"));
                
                Assert.NotNull(indexDefinition);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestUnlock()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1" };
                var dto2 = new Dto() { Name = "Name2" };

                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
                
                var index = new DummyIndex();

                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                store.Maintenance.Send(new SetIndexesLockOperation("DummyIndex", IndexLockMode.Unlock));
                
                store.Maintenance.Send(new DeleteIndexOperation("DummyIndex"));
                
                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("DummyIndex"));
                
                Assert.Null(indexDefinition);
            }
        }
    }
    
    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyIndex()
        {
            Map = orders => from order in orders
                select new Dto { Name = $"{order.Name}_new" };
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
}
