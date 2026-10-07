using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes.Static;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26440 : RavenTestBase
    {
        public RavenDB_26440(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_With_Method_Call_Chain_Above_The_Limit_Should_Fail_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var chainDepth = IndexCompiler.MaxAllowedInvocationChainDepth + 1;
                var indexDefinition = new IndexDefinition
                {
                    Name = "DeepChainIndex",
                    Maps = { CreateMapWithConcatChain(concatCalls: chainDepth - 1) }
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains($"The map function contains a chain of {chainDepth} method calls", ex.Message);
                Assert.Contains($"maximum allowed chain depth of {IndexCompiler.MaxAllowedInvocationChainDepth}", ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Reduce_With_Method_Call_Chain_Above_The_Limit_Should_Fail_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var chainDepth = IndexCompiler.MaxAllowedInvocationChainDepth + 1;
                var concatChain = string.Concat(Enumerable.Repeat(".Concat(g.SelectMany(x => x.Tags))", chainDepth - 2)); // plus the opening SelectMany() and the closing ToArray()
                var indexDefinition = new IndexDefinition
                {
                    Name = "DeepChainMapReduceIndex",
                    Maps = { "from user in docs.Users select new { user.Name, user.Tags }" },
                    Reduce = $"from result in results group result by result.Name into g select new {{ Name = g.Key, Tags = g.SelectMany(x => x.Tags){concatChain}.ToArray() }}"
                };

                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(indexDefinition)));
                Assert.Contains($"The reduce function contains a chain of {chainDepth} method calls", ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_With_Method_Call_Chain_At_The_Limit_Should_Compile_And_Index()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "DeepChainIndex",
                    Maps = { CreateMapWithConcatChain(concatCalls: IndexCompiler.MaxAllowedInvocationChainDepth - 1) }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "John", Tags = new List<string> { "a", "b" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexDefinition.Name }));
                Assert.Empty(indexErrors[0].Errors);

                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.DocumentQuery<User>(indexDefinition.Name)
                        .WhereEquals("Values", "a")
                        .ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("John", users[0].Name);
                }
            }
        }

        private static string CreateMapWithConcatChain(int concatCalls)
        {
            // the chain depth seen by the index compiler is concatCalls + 1, the closing ToArray() is a call in the same chain
            var concatChain = string.Concat(Enumerable.Repeat(".Concat(user.Tags ?? Enumerable.Empty<string>())", concatCalls));
            return $"from user in docs.Users select new {{ Values = (user.Tags ?? Enumerable.Empty<string>()){concatChain}.ToArray() }}";
        }

        private class User
        {
            public string Name { get; set; }

            public List<string> Tags { get; set; }
        }
    }
}
