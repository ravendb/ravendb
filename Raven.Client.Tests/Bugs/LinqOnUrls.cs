using System;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Http;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
    public class LinqOnUrls : RemoteClientTest, IDisposable
	{
		private readonly string path;
        private readonly int port;

		public LinqOnUrls()
		{
            port = 8080;
            path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
		}


		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

        [Fact]
        public void CanQueryUrlsValuesUsingLinq()
        {
            using (GetNewServer(port, path))
            {
                var documentStore = new DocumentStore {Url = "http://localhost:" + port};
                documentStore.Initialize();

                var documentSession = documentStore.OpenSession();

                documentSession.Query<User>().Where(
                    x => x.Name == "http://www.idontexistinthecacheatall.com?test=xxx&gotcha=1")
                    .FirstOrDefault();
            }
        }
	}
}