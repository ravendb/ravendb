using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;
using Raven.Client.Connection;

namespace Raven.Tests.NotModified
{
	/// <summary>
	/// The tests in this class test that 304 is returned when nothing at all is changed, and that 200 is returned when the
	/// object being tested is changed; they do not test what happens when other objects are changed
	/// </summary>
	public class NotModified : RemoteClientTest
	{
		[Fact]
		public void ServerReturnsNotModifiedWhenAppropriateForDocument()
		{
			RunNotModifiedTestsForUrl(
				new User { Id = "users/ayende", Name = "Ayende" },
				new User { Id = "users/ayende", Name = "Rahien" },
				"http://localhost:8079/docs/users/ayende"
			);
		}

		[Fact]
		public void ServerReturnsNotModifiedWhenAppropriateForAllDocs()
		{
			RunNotModifiedTestsForUrl(
				new User { Id = "users/ayende", Name = "Ayende" },
				new User { Id = "users/rahien", Name = "Rahien" },
				"http://localhost:8079/docs/"
			);
		}

		[Fact]
		public void ServerReturnsNotModifiedWhenAppropriateForDatabases()
		{
			RunNotModifiedTestsForUrl(
				new { Id = "Raven/Databases/FirstDatabase" },
				new { Id = "Raven/Databases/SecondDatabase" },
				"http://localhost:8079/databases/"
			);
		}

		private void RunNotModifiedTestsForUrl(object firstItemToStore, object secondItemToStore, string url)
		{
			using (GetNewServer())
			using (var docStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				// Store an item
				Guid? firstEtag;
				using (var session = docStore.OpenSession())
				{
					session.Store(firstItemToStore);
					session.SaveChanges();
					firstEtag = session.Advanced.GetEtagFor(firstItemToStore);
				}

				// Here, we should get the same etag we got when we asked the session
				var getRequest = WebRequest.Create(url);
				using (var response = GetHttpResponseHandle304(getRequest))
				{
					Assert.Equal(HttpStatusCode.OK, response.StatusCode);
					Assert.Equal(firstEtag, response.GetEtagHeader());
				}

				// If we ask with If-None-Match (and it's a match), we'll get 304 Not Modified
				getRequest = WebRequest.Create(url);
				getRequest.Headers.Add("If-None-Match", firstEtag.ToString());
				using (var response = GetHttpResponseHandle304(getRequest))
				{
					Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
				}

				// Change the item or add a second item
				Guid? secondEtag;
				using (var session = docStore.OpenSession())
				{
					session.Store(secondItemToStore);
					session.SaveChanges();
					secondEtag = session.Advanced.GetEtagFor(secondItemToStore);
				}

				// If we ask with the old etag, we'll get a new result
				getRequest = WebRequest.Create(url);
				getRequest.Headers.Add("If-None-Match", firstEtag.ToString());
				using (var response = GetHttpResponseHandle304(getRequest))
				{
					Assert.Equal(HttpStatusCode.OK, response.StatusCode);
					Assert.Equal(secondEtag, response.GetEtagHeader());
				}

				// If we ask with the new etag, we'll get 304 Not Modified
				getRequest = WebRequest.Create(url);
				getRequest.Headers.Add("If-None-Match", secondEtag.ToString());
				using (var response = GetHttpResponseHandle304(getRequest))
				{
					Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
				}
			}
		}

		private HttpWebResponse GetHttpResponseHandle304(WebRequest request)
		{
			try
			{
				return (HttpWebResponse)request.GetResponse();
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response != null && response.StatusCode == HttpStatusCode.NotModified)
				{
					return response;
				}

				throw;
			}
		}
	}
}
