using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class HttpClientCouldReachLocalhost
	{
		[TestMethod]
		public async Task CanReachInternet()
		{
			var httpClient = new HttpClient();
			var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, "http://www.google.com");
			var result = await httpClient.SendAsync(httpRequestMessage);
			Assert.AreEqual(HttpStatusCode.OK, result.StatusCode);
		}

		[TestMethod]
		public async Task CanReachLocalhost()
		{
			var httpClient = new HttpClient();
			var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, "http://127.0.0.1:8079/stats");
			var result = await httpClient.SendAsync(httpRequestMessage);
			Assert.AreEqual(HttpStatusCode.OK, result.StatusCode);
		}
	}
}