using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class FirstOrDefault : RavenTestBase
    {
        public FirstOrDefault(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class IndexWithLetInReduceFunction : AbstractIndexCreationTask<User, IndexWithLetInReduceFunction.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public string Name { get; set; }
            }

            public IndexWithLetInReduceFunction()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Id,
                                   user.Name
                               };

                Reduce = results => from result in results
                                    group result by result.Id
                                    into g
                                    let dummy = g.First(x => x.Name != null)
                                    select new
                                    {
                                        Id = g.Key,
                                        dummy.Name,
                                    };
            }
        }

        [Fact]
        public void WillReplaceFirstWithFirstOrDefault()
        {
            var indexDefinition = new IndexWithLetInReduceFunction { Conventions = new DocumentConventions() }.CreateIndexDefinition();
            Assert.Contains("FirstOrDefault", indexDefinition.Reduce);
        }
    }
}
