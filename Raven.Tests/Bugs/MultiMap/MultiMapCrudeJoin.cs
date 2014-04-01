using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.MultiMap
{
    public class MultiMapCrudeJoin : RavenTest
    { 
        [Fact]
        public void CanUseMultimapAsASimpleJoin()
        {
            using (var store = NewDocumentStore())
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

        public class Root
        {
            public int RootId;
        }
        public class Other
        {
            public int OtherId;
            public int RootId;
        }

        public class CrudeJoinResult
        {
            public int RootId;
            public Root TheRoot;
            public Other[] Others;
        }

        public class CrudeJoinTask : AbstractMultiMapIndexCreationTask<CrudeJoinResult>
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
            }
        }
    }
}
