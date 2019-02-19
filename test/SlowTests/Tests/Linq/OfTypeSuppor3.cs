// -----------------------------------------------------------------------
//  <copyright file="OfTypeSupport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class OfTypeSupport3 : RavenTestBase
    {
        [Fact]
        public void OfTypeSupportSelectAfterwards()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Values = new[] { new Bar() { Value = "str" } } });
                    session.SaveChanges();
                }

                store.ExecuteIndex(new Index());

                using (var session = store.OpenSession())
                {
                    var item = session.Query<Index.Result, Index>()
                                      .Customize(c => c.WaitForNonStaleResults())
                                      .ProjectInto<Index.Result>()
                                      .Single();

                    Assert.NotNull(item.Strings);
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
