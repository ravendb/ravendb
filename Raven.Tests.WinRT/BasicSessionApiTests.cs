using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Client.Document;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class CanAccessWeb
	{
		[TestMethod]
		public async Task CanQueryGoogle2()
		{
			var httpClient = new HttpClient();
			var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, "http://127.0.0.1:8079/stats");
			var result = await httpClient.SendAsync(httpRequestMessage);
			Assert.AreEqual(HttpStatusCode.OK, result.StatusCode);
		}
	}

	[TestClass]
	public class BasicSessionApiTests : RavenTestBase
	{
		[TestMethod]
		public async Task CanQueryRavenDb()
		{
			var httpClient = new HttpClient();
			var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, "http://www.google.com");
			var result = await httpClient.SendAsync(httpRequestMessage);
			Assert.Equals(200, result.StatusCode);
		}



		[TestMethod]
		public async Task CanSaveAndLoad()
		{
			using (var store = new DocumentStore {Url = Url}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new User { Name = "Fitzchak" });
					await session.SaveChangesAsync();
				}

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.LoadAsync<User>("users/1");
					Assert.IsNotNull(user);
					Assert.AreEqual("Fitzchak", user.Name);
				}
			}
		}

		private class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}