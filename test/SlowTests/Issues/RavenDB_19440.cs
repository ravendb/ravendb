using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19440 : RavenTestBase
{
    public RavenDB_19440(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void QueryWillThrowWhenPerformingSelectAfterProjectInto()
    {
        using var store = GetDocumentStore();
        {
            using var session = store.OpenSession();
            session.Store(new Data() {Name = "Test"});
            session.SaveChanges();
        }
        var index = new DataIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        {
            using var session = store.OpenSession();
            var query = (IQueryable<Data>)session.Query<Data, DataIndex>();
            var q = query
                .ProjectInto<Dto>()
                .Select(i => new Dto() {Id = i.Id, Name = i.Name});
            AssertResult(() => q.ToString());
        }
    }

    [Fact]
    public void QueryWillThrowWhenTwoProjectionsAreIncludedInQuery()
    {
        using var store = GetDocumentStore();


        using var session = store.OpenSession();
        var q = session.Query<DtoExt>().Customize(customization => customization.WaitForNonStaleResults())
            .Where(i => i.SecondName == "kaszebe")
            .Select(i => new Dto2Ext() {Name = i.Name, SecondName = i.SecondName})
            .Select(i => new DtoExt() {Name = i.SecondName, SecondName = i.Name});
        AssertResult(() => q.ToString());
    }

    private void AssertResult(Action queryToString)
    {
        var exception = Assert.ThrowsAny<InvalidOperationException>(queryToString);
        Assert.True(exception.Message.Contains("Projection is already done. You should not project your result twice."));
    }
    
    private class DtoExt
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string SecondName { get; set; }
    }

    private class Dto2Ext
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string SecondName { get; set; }
    }


    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }


    private class Data
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class DataIndex : AbstractIndexCreationTask<Data>
    {
        public DataIndex()
        {
            Map = datas => datas.Select(i => new {Name = i.Name});
        }
    }
}
