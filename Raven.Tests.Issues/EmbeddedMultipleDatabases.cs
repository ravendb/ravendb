// -----------------------------------------------------------------------
//  <copyright file="EmbeddedMultipleDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class EmbeddedMultipleDatabases : RavenTest
    {
        [Fact]
        public void ShouldWork1()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore())
            {
                store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Abc");
                store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Cba");

                var abcCommands = store.DatabaseCommands.ForDatabase("Abc");
                var cbaCommands = store.DatabaseCommands.ForDatabase("Cba");

                abcCommands.Put("abc/1", null, new RavenJObject(), new RavenJObject());
                cbaCommands.Put("cba/1", null, new RavenJObject(), new RavenJObject());

                Assert.NotNull(abcCommands.Get("abc/1"));
                Assert.Null(abcCommands.Get("cba/1"));

                Assert.NotNull(cbaCommands.Get("cba/1"));
                Assert.Null(cbaCommands.Get("abc/1"));
            }
        }

        [Fact]
        public void ShouldWork2()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore(configureStore: documentStore => documentStore.DefaultDatabase = "test"))
            {
                var documentDatabase = store.DocumentDatabase;
                var systemDatabase = store.SystemDatabase;

                documentDatabase.Documents.Put("abc/1", null, new RavenJObject(), new RavenJObject(), null);
                systemDatabase.Documents.Put("cba/1", null, new RavenJObject(), new RavenJObject(), null);

                Assert.NotNull(documentDatabase.Documents.Get("abc/1", null));
                Assert.Null(documentDatabase.Documents.Get("cba/1", null));

                Assert.NotNull(systemDatabase.Documents.Get("cba/1", null));
                Assert.Null(systemDatabase.Documents.Get("abc/1", null));
            }
        }
    }
}
