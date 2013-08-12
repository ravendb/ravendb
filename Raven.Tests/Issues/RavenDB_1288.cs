using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Issues
{
    public class RavenDB_1288 : RavenTest
    {
        #region Test Related Definitions

        private class FooTransformer : AbstractTransformerCreationTask<Foo>
        {
            public FooTransformer()
            {
                TransformResults = docs => docs.Select(doc => new FooTransformed
                {
                    FirstLetter = doc.FirstLetter
                });
            }
        }

        private class FooTransformed
        {
            public string FirstLetter { get; set; }
        }

        private class Foo
        {
            public string Id { get; set; }
            public string Value { get; set; }

            public string FirstLetter
            {
                get { return Value[0].ToString(); }
            }

            public Foo(string value)
            {
                Value = value;
                Id = "foos/" + value.ToLower();
            }
        }

        private class Index : AbstractIndexCreationTask<Foo, Index.Result>
        {
            public class Result
            {
                public string Stuff { get; set; }
                public string StuffedLetter { get; set; }
            }

            public Index()
            {
                Map = docs => docs.Select(doc => new Result
                {
                    StuffedLetter = doc.FirstLetter,
                    Stuff = doc.Value
                });
            }
        }

        #endregion

        [Fact]
        public void ResultsTRansformer_LazyQuery_InvalidCastException()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new Index());
                new FooTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Foo("abcdef"));
                    session.Store(new Foo("aabcde"));
                    session.Store(new Foo("acdefg"));
                    session.Store(new Foo("bcdefg"));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    IEnumerable<FooTransformed> result = null;
                    Assert.DoesNotThrow(() => result =
                        session.Query<Index.Result, Index>()
                               .Customize(c => c.WaitForNonStaleResults())
                               .Where(f => f.StuffedLetter == "a")
                               .TransformWith<FooTransformer, FooTransformed>()
                               .Lazily().Value // if you remove this, it works fine
                               );
                }
            }
        }
    }
}