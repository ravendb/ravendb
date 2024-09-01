// -----------------------------------------------------------------------
//  <copyright file="OfTypeSupport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class OfTypeSupport3 : RavenTestBase
    {
        public OfTypeSupport3(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OfTypeSupportSelectAfterwards(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Values = new[] { new Bar() { Value = "str" } } });
                    session.SaveChanges();
                }

                store.ExecuteIndex(new Index());

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var item = session.Query<Index.Result, Index>()
                                      .Customize(c => c.WaitForNonStaleResults())
                                      .ProjectInto<Index.Result>()
                                      .Single();

                    Assert.NotNull(item.Strings);
                    Assert.NotNull(item.Values);
                }
            }
        }

        private class Index : AbstractIndexCreationTask<Foo, Index.Result>
        {
            public class Result
            {
                public object[] Values { get; set; }
                public string[] Strings { get; set; }
            }

            public Index()
            {
                Map = docs => docs.Select(doc => new Result
                                                 {
                                                     Values = doc.Values,
                                                     Strings = doc.Values.OfType<Bar>().Select(x => x.Value).ToArray(),
                                                 });

                Store(result => result.Strings, FieldStorage.Yes);

                Store(x => x.Values, FieldStorage.Yes);
                Index(x => x.Values, FieldIndexing.No);
            }

        }

        private class Foo
        {
            public object[] Values { get; set; }

        }

        private class Bar
        {
            public string Value { get; set; }
        }
    }
}
