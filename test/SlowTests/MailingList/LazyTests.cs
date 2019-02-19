// -----------------------------------------------------------------------
//  <copyright file="LazyTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class LazyTests : RavenTestBase
    {
        private class Simple
        {
            public Simple()
            {
                stamp = DateTime.UtcNow.Second;
            }

            public string Id { get; set; }
            public string key { get; set; }
            public int stamp { get; set; }
        }

        private class Reduced
        {
            public string key { get; set; }
            public int stamp { get; set; }
        }


        private class Simple_Index : AbstractIndexCreationTask<Simple>
        {
            public Simple_Index()
            {
                Map = entities => from entity in entities
                                  select new { entity.key, entity.stamp };
            }
        }


        private static void Populate(IDocumentStore store)
        {
            new Simple_Index().Execute(store);
            using (var session = store.OpenSession())
            {
                QueryStatistics stats = null;
                session.Query<Simple, Simple_Index>().Statistics(out stats).Take(0).ToArray();
                if (stats.TotalResults > 0) return;
            }

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 1000; i++)
                    session.Store(new Simple());
                session.SaveChanges();
            }

            WaitForIndexing(store);
        }

        [Fact]
        public void Passing_not_embedded_with_empty_convention()
        {
            using (var store = GetDocumentStore())
            {
                Populate(store);

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }
            }
        }

        [Fact]
        public void Passing_embedded_disabled_profiling_false()
        {
            using (var store = GetDocumentStore())
            {
                Populate(store);

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }
            }
        }

        [Fact]
        public void Passing_not_embedded_with_disabled_profiling_true()
        {
            using (var store = GetDocumentStore())
            {
                Populate(store);

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }
            }
        }

        [Fact]
        public void Passing_when_not_using_lazy()
        {
            using (var store = GetDocumentStore())
            {
                Populate(store);

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).ToArray();
                    Assert.True(dump.Length > 0);
                }

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).ToArray();
                    Assert.True(dump.Length > 0);
                }
            }
        }

        [Fact]
        public void Failing_when_not_embedded_with_disabled_profiling_false()
        {
            using (var store = GetDocumentStore())
            {
                Populate(store);

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }

                using (var session = store.OpenSession())
                {
                    var dump = session.Query<Simple, Simple_Index>().Take(1024).Lazily().Value.ToArray();
                    Assert.True(dump.Length > 0);
                }
            }
        }
    }
}
