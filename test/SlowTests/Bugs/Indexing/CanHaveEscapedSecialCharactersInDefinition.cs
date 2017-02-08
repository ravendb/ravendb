// -----------------------------------------------------------------------
//  <copyright file="CanHaveEscapedSecialCharactersInDefinition.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class SecialCharactersInDefinition : RavenNewTestBase
    {
        private const string FooIndexName = "SomeFooIndexWithSpecialCharacters";

        [Fact]
        public void CanContainSecialCharactersInDefinition()
        {
            using (var documentStore = GetDocumentStore())
            {
                new FooIndex().Execute(documentStore);
                Assert.NotNull(documentStore.Admin.Send(new GetIndexOperation(FooIndexName)));
            }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = docs => from foo in docs
                              select new
                              {
                                  Text = string.Join("\n\r\'\b\"\\\t\v\u0013\u1567\0", foo.Title, foo.Description),

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
