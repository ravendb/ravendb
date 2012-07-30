//-----------------------------------------------------------------------
// <copyright file="WillNotFailSystemIfServerIsNotAvailableOnStartup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WillNotFailSystemIfServerIsNotAvailableOnStartup : RemoteClientTest, IDisposable
	{
		private readonly string path;

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		public WillNotFailSystemIfServerIsNotAvailableOnStartup()
		{
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);
		}

		[Fact]
		public void CanStartWithoutServer()
		{
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				Assert.Throws<WebException>(() => store.OpenSession().Load<User>("user/1"));
				using (GetNewServer(8079,path))
				{
					using (var session = store.OpenSession())
					{
						Assert.Null(session.Load<Item>("items/1"));
					}
				}
			}
		}
	}
}