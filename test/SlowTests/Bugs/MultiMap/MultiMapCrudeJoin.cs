using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.MultiMap
{
    public class MultiMapCrudeJoin : RavenTestBase
    {
        public MultiMapCrudeJoin(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanUseMultimapAsASimpleJoin(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new CrudeJoinTask().Execute(store);

                using (var documentSession = store.OpenSession())
                {
                    documentSession.Store(new Root { RootId = 1 });
                    documentSession.Store(new Other { RootId = 1, OtherId = 2 });
                    documentSession.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<CrudeJoinResult, CrudeJoinTask>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(1, results.Single().RootId);
                    Assert.Equal(1, results.Single().TheRoot.RootId);
                    Assert.Equal(1, results.Single().Others.Single().RootId);
                    Assert.Equal(2, results.Single().Others.Single().OtherId);
                }
            }
        }

        private class Root
        {
            public int RootId;
        }

        private class Other
        {
            public int OtherId;
            public int RootId;
        }

        private class CrudeJoinResult
        {
            public int RootId;
            public Root TheRoot;
            public Other[] Others;
        }

        private class CrudeJoinTask : AbstractMultiMapIndexCreationTask<CrudeJoinResult>
        {
            public CrudeJoinTask()
            {
                AddMap<Root>(roots => roots.Select(r => new CrudeJoinResult() { RootId = r.RootId, TheRoot = r, Others = new Other[0] }));
                AddMap<Other>(others => others.Select(o => new CrudeJoinResult() { RootId = o.RootId, TheRoot = null, Others = new Other[] { o } }));

                Reduce = results => from r in results
                                    group r by r.RootId
                                        into g
                                    select new CrudeJoinResult()
                                    {
                                        RootId = g.Key,
                                        TheRoot = g.Select(it => it.TheRoot).FirstOrDefault(it => it != null),
                                        Others = g.SelectMany(it => it.Others).ToArray()
                                    };
                
                Stores.Add(i=> i.TheRoot, FieldStorage.Yes);
                Stores.Add(i=> i.Others, FieldStorage.Yes);
                Index(i => i.TheRoot, FieldIndexing.No);
                Index(i => i.Others, FieldIndexing.No);
            }
        }
    }
}
