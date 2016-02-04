// -----------------------------------------------------------------------
//  <copyright file="NonHttpBackupRestore.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Smuggler.Embedded;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Files;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class NonHttpBackupRestore : RavenTest
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task CanImportFromDumpFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), "backup.ravendump");
            using (var store = NewDocumentStoreWithData())
            {
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(), 
                    new DatabaseSmugglerEmbeddedSource(store.SystemDatabase), 
                    new DatabaseSmugglerFileDestination(file));

                await smuggler.ExecuteAsync();
            }

            using (var store = NewDocumentStore())
            {
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(),
                    new DatabaseSmugglerFileSource(file), 
                    new DatabaseSmugglerEmbeddedDestination(store.SystemDatabase));

                await smuggler.ExecuteAsync();

                using (var session = store.OpenSession())
                {
                    // Person imported.
                    Assert.Equal(1, session.Query<Person>().Customize(x => x.WaitForNonStaleResults()).Take(5).Count());
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ImportReplacesAnExistingDatabase()
        {
            var file = Path.GetTempFileName();

            using (var store = NewDocumentStoreWithData())
            {
                var smuggler = new DatabaseSmuggler(
                   new DatabaseSmugglerOptions(),
                   new DatabaseSmugglerEmbeddedSource(store.SystemDatabase),
                   new DatabaseSmugglerFileDestination(file));

                await smuggler.ExecuteAsync();

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>(1);
                    person.Name = "Sean Kearon";

                    session.Store(new Person { Name = "Gillian" });

                    session.SaveChanges();
                }

                smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(),
                    new DatabaseSmugglerFileSource(file),
                    new DatabaseSmugglerEmbeddedDestination(store.SystemDatabase));

                await smuggler.ExecuteAsync();

                using (var session = store.OpenSession())
                {
                    // Original person has been restored.
                    Assert.NotNull(session.Query<Person, PeopleByName>().Customize(x => x.WaitForNonStaleResults()).Single(x => x.Name == "Sean"));

                    // The newly added person has not been removed.
                    Assert.True(session.Query<Person, PeopleByName>().Customize(x => x.WaitForNonStaleResults()).Any(x => x.Name == "Gillian"));
                }
            }
        }

        protected override void CreateDefaultIndexes(IDocumentStore documentStore)
        {
            base.CreateDefaultIndexes(documentStore);
            new PeopleByName().Execute(documentStore);
        }

        protected byte[] ReadFully(Stream input)
        {
            var buffer = new byte[16 * 1024];
            using (var ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }

        private EmbeddableDocumentStore NewDocumentStoreWithData()
        {
            var store = NewDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Person { Name = "Sean" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                // Ensure the index is built.
                var people = session.Query<Person, PeopleByName>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Name == "Sean")
                    .ToArray();
                Assert.NotEmpty(people);
            }

            return store;
        }

        public class PeopleByName : AbstractIndexCreationTask<Person>
        {
            public PeopleByName()
            {
                Map = (persons => from person in persons
                                  select new
                                  {
                                      person.Name,
                                  });
            }
        }

        public class Person
        {
            public string Id;
            public string Name;
        }
    }
}
