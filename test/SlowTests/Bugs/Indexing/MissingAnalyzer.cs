//-----------------------------------------------------------------------
// <copyright file="MissingAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class MissingAnalyzer : RavenTestBase
    {
        public MissingAnalyzer(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_give_clear_error_when_starting()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.Name }" },
                    Fields =
                    {
                        {"Name", new IndexFieldOptions {Analyzer = "foo bar"}}
                    },
                    Name = "foo"
                })));

                Assert.Contains("Cannot find analyzer type 'foo bar' for field: Name", e.Message);
            }
        }
    }
}
