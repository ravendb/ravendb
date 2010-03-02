using System;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Rhino.DivanDB.Server;
using Xunit;

namespace Rhino.DivanDB.Client.Tests
{
    public class DocumentStoreServerTests : BaseTest
    {
        [Fact]
        public void Should_insert_into_db_and_set_id()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (var server = new DivanServer(DbName, 8080))
            {
                var documentStore = new DocumentStore("localhost", 8080);
                documentStore.Initialise();

                var session = documentStore.OpenSession();
                var entity = new Company { Name = "Pap" };
                session.Store(entity);

                Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
            }
        }
    }
}