using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Server;
using Xunit;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using System.Reflection;
using System.IO;
using Raven.Server;
using System.Threading;

namespace Raven.Client.Tests.Document
{
    public class TotalCountServerTest : BaseTest, IDisposable
    {
		private readonly string path;
        private readonly int port;

        public TotalCountServerTest()
		{
            port = 8080;
            path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
		}

		#region IDisposable Members

		public void Dispose()
		{
			Thread.Sleep(100);
			Directory.Delete(path, true);
		}

		#endregion

        [Fact]
        public void TotalResultIsIncludedInQueryResult()
        {
            using (var server = GetNewServer(port, path))
            {
                using (var store = new DocumentStore { Url = "http://localhost:" + port })
                {
                    store.Initialize();

                    using (var session = store.OpenSession())
                    {
                        Company company1 = new Company()
                        {
                            Name = "Company1",
                            Address1 = "",
                            Address2 = "",
                            Address3 = "",
                            Contacts = new List<Contact>(),
                            Phone = 2
                        };
                        Company company2 = new Company()
                        {
                            Name = "Company2",
                            Address1 = "",
                            Address2 = "",
                            Address3 = "",
                            Contacts = new List<Contact>(),
                            Phone = 2
                        };

                        session.Store(company1);
                        session.Store(company2);
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        int resultCount = session.LuceneQuery<Company>().WaitForNonStaleResults().QueryResult.TotalResults;
                        Assert.Equal(2, resultCount);
                    }
                }
            }
              
           

        }
    }
}
