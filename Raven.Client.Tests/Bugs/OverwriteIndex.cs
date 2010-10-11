using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Raven.Database;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Server;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class OverwriteIndexLocally : LocalClientTest
    {
        [Fact]
        public void CanOverwriteIndex()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Name }"
                                                }, overwrite:true);


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
