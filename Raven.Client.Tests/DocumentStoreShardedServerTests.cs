using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Database;
using Raven.Server;
using Xunit;
using System.Collections.Generic;

namespace Raven.Client.Tests
{
	public class DocumentStoreShardedServerTests : BaseTest, IDisposable
	{
        private readonly string path1;
        private readonly string path2;
        private readonly int port1;
        private readonly int port2;
        private readonly string server;

        public DocumentStoreShardedServerTests()
		{
            server = "localhost";

            port1 = 8080;
            port2 = 8081;

            path1 = GetPath("TestDb1");
            path2 = GetPath("TestDb2");

            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port1);
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port2);
        }

        [Fact]
        public void Can_insert_into_two_servers_running_simultaneously_without_sharding()
        {
            var serversStoredUpon = new List<int>();

            using (var server1 = GetNewServer(port1, path1))
            using (var server2 = GetNewServer(port2, path2))
            {
                foreach (var port in new[] { port1, port2 })
                {
                    using (var documentStore = new DocumentStore(server, port).Initialise())
                    using (var session = documentStore.OpenSession())
                    {
                        documentStore.Stored += (storeServer, storePort, storeEntity) => serversStoredUpon.Add(storePort);

                        var entity = new Company { Name = "Company" };
                        session.Store(entity);
                        Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
                    }
                }
            }

            Assert.Equal(port1, serversStoredUpon[0]);
            Assert.Equal(port2, serversStoredUpon[1]);
        }

        #region IDisposable Members

        public void Dispose()
        {
            Thread.Sleep(100);

            foreach (var path in new[] { path1, path2 })
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch (Exception) { }
            }
        }

        #endregion

    }
}