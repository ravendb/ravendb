//-----------------------------------------------------------------------
// <copyright file="TotalCountServerTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Database.Extensions;
using Raven.Database.Server;
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
			port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);
		}

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		[Fact]
		public void TotalResultIsIncludedInQueryResult()
		{
			using (var server = GetNewServer(port, path))
			{
				using (var store = new DocumentStore { Url = "http://localhost:" + port }.Initialize())
				{
					using (var session = store.OpenSession())
					{
						var company1 = new Company()
						{
							Name = "Company1",
							Address1 = "",
							Address2 = "",
							Address3 = "",
							Contacts = new List<Contact>(),
							Phone = 2
						};
						var company2 = new Company()
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