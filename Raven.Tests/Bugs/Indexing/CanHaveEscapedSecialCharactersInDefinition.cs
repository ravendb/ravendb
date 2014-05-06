// -----------------------------------------------------------------------
//  <copyright file="CanHaveEscapedSecialCharactersInDefinition.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Indexing {
    public class SecialCharactersInDefinition : RavenTest
    {
        const string FooIndexName = "SomeFooIndexWithSpecialCharacters";

        [Fact]
        public void CanContainSecialCharactersInDefinition() {
            using (var documentStore = this.NewDocumentStore()) {
                new FooIndex().Execute(documentStore);

                Assert.NotNull(documentStore.DatabaseCommands.GetIndex(FooIndexName));
            }
        }

        public class FooIndex : AbstractIndexCreationTask<Foo> {
            public FooIndex() {
                this.Map = docs => from foo in docs
                                   select new
                                   {
                                       Text = string.Join("\n\r\'\b\"\\\t\v\u0013\u1567\0", foo.Title, foo.Description),
                                       Chars = new[]
                                       {
                                           '\n', '\r', '\'', '\b', '\"', '\\', '\t', '\v', '\u0013', '\u1567', '\0'
                                       }
                                   };
            }

            public override string IndexName {
                get { return FooIndexName; }
            }
        }

        public class Foo {
            public string Title { get; set; }
            public string Description { get; set; }
        }
    }
}