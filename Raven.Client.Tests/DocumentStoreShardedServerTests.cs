using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Database;
using Raven.Server;
using Xunit;

namespace Raven.Client.Tests
{
	public class DocumentStoreShardedServerTests : BaseTest, IDisposable
	{
		private readonly string path1;
        private readonly string path2;
        private readonly int port1 = 8080;
        private readonly int port2 = 8081;

        public DocumentStoreShardedServerTests()
		{
            path1 = GetPath("TestDb1");
            path2 = GetPath("TestDb2");
		}

        [Fact]
        public void Can_insert_into_two_servers_running_simultaneously_without_sharding()
        {
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port1);
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port2);
            using (var server1 = GetNewServer(port1, path1))
            using (var server2 = GetNewServer(port2, path2))
            {
                foreach (var port in new[] { port1, port2 })
                {
                    var documentStore = new DocumentStore("localhost", port);
                    documentStore.Initialise();

                    var session = documentStore.OpenSession();
                    var entity = new Company { Name = "Company" };
                    session.Store(entity);

                    Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
                }
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            Thread.Sleep(100);

            try
            {
                Directory.Delete(path1, true);
            } catch (Exception) { }

            try
            {
                Directory.Delete(path2, true);
            } catch (Exception) { }
        }

        #endregion

    }
}