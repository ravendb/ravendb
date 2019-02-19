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
    public class OfTypeSupport2 : RavenTestBase
    {
        [Fact]
        public void ShouldCorrectlyMatchTheTypeName()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Values = new[] { new Bar() } });
                    session.SaveChanges();
                }

                store.ExecuteIndex(new Index());

                using (var session = store.OpenSession())
                {
                    var item = session.Query<Index.Result, Index>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .ProjectInto<Index.Result>()
                        .Single();

                    Assert.NotNull(item.Bars);
                }
            }
        }

        private class Index : AbstractIndexCreationTask<Foo, Index.Result>
        {
            public class Result
            {
                public object[] Values { get; set; }
                public Bar[] Bars { get; set; }
            }

            public Index()
            {
                Map = docs => docs.Select(doc => new Result
                {
                    Values = doc.Values,
                    Bars = doc.Values.OfType<Bar>().ToArray(),
                });

                Store(result => result.Bars, FieldStorage.Yes);
            }

        }

        private class Foo
        {
            public object[] Values { get; set; }

        }

        private class Bar
        {
        }
    }
}
