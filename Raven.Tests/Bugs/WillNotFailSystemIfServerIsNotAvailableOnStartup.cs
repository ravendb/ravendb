//-----------------------------------------------------------------------
// <copyright file="WillNotFailSystemIfServerIsNotAvailableOnStartup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WillNotFailSystemIfServerIsNotAvailableOnStartup : RemoteClientTest
	{
		[Fact]
		public void CanStartWithoutServer()
		{
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					Assert.Throws<WebException>(() => session.Load<User>("user/1"));
				}

				using (GetNewServer())
				{
					using (var session = store.OpenSession())
					{
						Assert.Null(session.Load<Item>("items/1"));
					}
				}
			}
		}

		[Fact]
		public async Task CanStartWithoutServerAsync()
		{
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					await AssertAsync.Throws<WebException>(() => session.LoadAsync<User>("user/1"));
				}

				using (GetNewServer())
				{
					using (var session = store.OpenAsyncSession())
					{
						Assert.Null(session.LoadAsync<Item>("items/1"));
					}
				}
			}
		}
	}
}