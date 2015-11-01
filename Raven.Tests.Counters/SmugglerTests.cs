using System;
using System.IO;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Files;
using Raven.Smuggler.Database.Remote;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class SmugglerTests : RavenTest
    {
        public class Foo
        {
            public string Id { get; set; }
            public DateTime Created { get; set; }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void DateTimePreserved()
        {
            var file = Path.GetTempFileName();

            try
            {
                var docId = string.Empty;

                using (var documentStore = NewRemoteDocumentStore())
                {
                    using (var session = documentStore.OpenSession())
                    {
                        var foo = new Foo {Created = DateTime.Today};
                        session.Store(foo);
                        docId = foo.Id;
                        session.SaveChanges();
                    }

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = documentStore.Url,
                            Database = documentStore.DefaultDatabase
                        }),
                        new DatabaseSmugglerFileDestination(file));

                    smuggler.Execute();
                }

                using (var documentStore = NewRemoteDocumentStore())
                {
                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerFileSource(file), 
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = documentStore.Url,
                            Database = documentStore.DefaultDatabase
                        }));

                    smuggler.Execute();

                    using (var session = documentStore.OpenSession())
                    {
                        var created = session.Load<Foo>(docId).Created;
                        Assert.False(session.Advanced.HasChanges);
                    }
                }
            }
            finally
            {
                if (File.Exists(file))
                {
                    File.Delete(file);
                }
            }
        }
    }
}

