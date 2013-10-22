using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;
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

        [Fact]
        public void DateTimePreserved()
        {
            var options = new SmugglerOptions {BackupPath = Path.GetTempFileName()};

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
                    var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions {Url = documentStore.Url});
                    smugglerApi.ExportData(options).Wait(TimeSpan.FromSeconds(15));
                }

                using (var documentStore = NewRemoteDocumentStore())
                {
                    var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions {Url = documentStore.Url});
                    smugglerApi.ImportData(options).Wait(TimeSpan.FromSeconds(15));
                    
                    using (var session = documentStore.OpenSession())
                    {
                        var created = session.Load<Foo>(docId).Created;
                        Assert.False(session.Advanced.HasChanges);
                    }
                }
            }
            finally
            {
                if (File.Exists(options.BackupPath))
                {
                    File.Delete(options.BackupPath);
                }
            }
        }
    }
}
