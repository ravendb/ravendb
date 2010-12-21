//-----------------------------------------------------------------------
// <copyright file="TotalCountServerTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Database.Extensions;
using Raven.Http;
using Xunit;
using Raven.Client.Document;

namespace Raven.Tests.Document
{
    public class TotalCountServerTest : RemoteClientTest, IDisposable
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
            IOExtensions.DeleteDirectory(path);
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
                        int resultCount = session.Advanced.LuceneQuery<Company>().WaitForNonStaleResults().QueryResult.TotalResults;
                        Assert.Equal(2, resultCount);
                    }
                }
            }
              
           

        }
    }
}
