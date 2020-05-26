using System;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15049 : RavenTestBase
    {
        public RavenDB_15049(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrowWhenIndexMapFunctionsHaveDifferentSourceTypes()
        {
            var e = Assert.Throws<InvalidOperationException>(() =>
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "MyIndex",
                    Maps =
                    {
                            { "from doc in docs.Companies select new { Name = doc.Name }" },
                            { "from counter in counters.Companies select new { Name = counter.Value }" },
                    }
                };

                var sourceType = indexDefinition.SourceType; // this should throw
            });

            Assert.Contains("Index definition cannot contain Maps with different source types.", e.Message);
        }
    }
}
