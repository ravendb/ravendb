//-----------------------------------------------------------------------
// <copyright file="MissingAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Raven.NewClient.Client.Exceptions.Compilation;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class MissingAnalyzer : RavenNewTestBase
    {
        [Fact(Skip = "Missing feature: RavenDB-6153")]
        public void Should_give_clear_error_when_starting()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Admin.Send(new PutIndexOperation("foo",
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs select new { doc.Name }" },
                        Fields =
                        {
                            {"Name", new IndexFieldOptions {Analyzer = "foo bar"}}
                        }

                    })));

                Assert.Equal("Cannot find analyzer type 'foo bar' for field: Name", e.Message);
            }
        }
    }
}
