using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_19646 : RavenTestBase
{
    public RavenDB_19646(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanDeleteAnalyzedDateFromCoraxIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Source() {Id = "doc/1", Tags = new List<string>() {"first", "second"}, PostedAt = new DateTime(2022, 12, 12)});
                session.Store(new Source() {Id = "doc/2", Tags = new List<string>() {"second", "third"}, PostedAt = new DateTime(2022, 12, 13)});
                session.SaveChanges();
            }
            var index = new CoraxMapReduce();
            index.Execute(store);
            Indexes.WaitForIndexing(store);
            using (var session = store.OpenSession())
            {
                var doc = session.Load<Source>("doc/1");
                doc.Tags.Remove("first");
                session.SaveChanges();
            }

            var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }));

            Assert.Empty(indexErrors.SelectMany(x => x.Errors));
        }
    }
    
    private class Source
    {
        public string Id { get; set; }
        public List<string> Tags { get; set; }
        
        public DateTime PostedAt { get; set; }
    }

    private class CoraxMapReduce : AbstractIndexCreationTask<Source, CoraxMapReduce.MapReduce>
    {
        public class MapReduce
        {
            public string Tag { get; set; }
            public long Count { get; set; }

            public DateTime PostedAt { get; set; }
        }

        public CoraxMapReduce()
        {
            Map = sources => from doc in sources
                from tag in doc.Tags
                select new MapReduce() {Tag = tag, Count = 1, PostedAt = doc.PostedAt};
            Reduce = sources =>
                from doc in sources
                group doc by doc.Tag
                into g
                select new MapReduce() {Count = g.Sum(i => i.Count), Tag = g.Key, PostedAt = g.Max(i => i.PostedAt)};
        }
    }
}
