using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11397 : RavenTestBase
    {
        private class InvalidMapReduce : AbstractIndexCreationTask<User, InvalidMapReduce.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public int Count { get; set; }
            }

            public InvalidMapReduce()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.Name,
                                   Count = 1
                               };

                Reduce = results => from r in results
                                    select new
                                    {
                                        r.Name,
                                        r.Count
                                    };
            }
        }

        [Fact]
        public void ReduceFunctionShouldContainGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => new InvalidMapReduce().Execute(store));
                Assert.Contains("Reduce function must contain a group by expression", e.Message);
            }
        }
    }
}
