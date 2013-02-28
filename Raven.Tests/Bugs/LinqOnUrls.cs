//-----------------------------------------------------------------------
// <copyright file="LinqOnUrls.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class LinqOnUrls : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly int port;

		public LinqOnUrls()
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
		public void CanQueryUrlsValuesUsingLinq()
		{
			using (GetNewServer(port, path))
			{
				using (var documentStore = new DocumentStore { Url = "http://localhost:" + port })
				{
					documentStore.Initialize();

					var documentSession = documentStore.OpenSession();

					documentSession.Query<User>().Where(
						x => x.Name == "http://www.idontexistinthecacheatall.com?test=xxx&gotcha=1")
						.FirstOrDefault();
				}
			}
		}
	}
}