// -----------------------------------------------------------------------
//  <copyright file="CanHaveEscapedSecialCharactersInDefinition.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class SecialCharactersInDefinition : RavenTestBase
    {
        public SecialCharactersInDefinition(ITestOutputHelper output) : base(output)
        {
        }

        private const string FooIndexName = "SomeFooIndexWithSpecialCharacters";

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanContainSecialCharactersInDefinition(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                new FooIndex().Execute(documentStore);
                Assert.NotNull(documentStore.Maintenance.Send(new GetIndexOperation(FooIndexName)));
            }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = docs => from foo in docs
                              select new
                              {
                                  Text = string.Join("\n\r\'\b\"\\\t\v\u0013\u1567\0", new[] { foo.Title, foo.Description }),

                                  Chars = new[]
                                  {
                                    '\n', '\r', '\'', '\b', '\"', '\\', '\t', '\v', '\u0013', '\u1567', '\0'
                                  }
                              };
            }

            public override string IndexName => FooIndexName;
        }

        private class Foo
        {
            public string Title { get; set; }
            public string Description { get; set; }
        }
    }
}
