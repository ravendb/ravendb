using System;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Http;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class OverwriteIndexRemotely : RemoteClientTest, IDisposable
    {
        private readonly string path;
        private readonly int port;

        public OverwriteIndexRemotely()
        {
            port = 8080;
            path = GetPath("TestDb");
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
        }

        #region IDisposable Members

        public void Dispose()
        {
            IOExtensions.DeleteDirectory(path);
        }

        #endregion

        [Fact]
        public void CanOverwriteIndex()
        {
            using (var server = GetNewServer(port, path))
            {
                var store = new DocumentStore { Url = "http://localhost:" + port };
                store.Initialize();

                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Name }"
                                                }, overwrite: true);


                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Name }"
                                                }, overwrite: true);

                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Email }"
                                                }, overwrite: true);

                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Email }"
                                                }, overwrite: true);
            }
        }
    }
}