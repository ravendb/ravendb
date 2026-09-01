using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
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
        public void Index_With_Deeply_Chained_Linq_Should_Fail_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var concatChain = string.Join("",
                    Enumerable.Range(0, 150).Select(_ => ".Concat(doc.Tags ?? Enumerable.Empty<string>())"));

                var map = $@"from doc in docs.Users
select new
{{
    Values = (doc.Tags ?? Enumerable.Empty<string>()){concatChain}.ToArray()
}}";

                var indexDefinition = new IndexDefinition
                {
                    Name = "DeepConcatIndex",
                    Maps = { map }
                };

                var ex = Assert.Throws<IndexCompilationException>(() =>
                    store.Maintenance.Send(new Raven.Client.Documents.Operations.Indexes.PutIndexesOperation(indexDefinition)));
                Assert.Contains("deeply chained", ex.Message, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
