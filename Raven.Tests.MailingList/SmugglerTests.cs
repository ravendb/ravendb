using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;
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

        [Fact]
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
                    var smugglerApi = new SmugglerApi();
					smugglerApi.ExportData(new SmugglerExportOptions { ToFile = file, From = new RavenConnectionStringOptions { Url = documentStore.Url, DefaultDatabase = documentStore.DefaultDatabase } }, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));
                }

                using (var documentStore = NewRemoteDocumentStore())
                {
                    var smugglerApi = new SmugglerApi();
					smugglerApi.ImportData(new SmugglerImportOptions { FromFile = file, To = new RavenConnectionStringOptions { Url = documentStore.Url, DefaultDatabase = documentStore.DefaultDatabase } }, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));
                    
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
