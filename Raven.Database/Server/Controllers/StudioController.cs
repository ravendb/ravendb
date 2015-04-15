using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Web.Http;

using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class StudioController : RavenDbApiController
	{
		[HttpGet]
		[RavenRoute("raven")]
		[RavenRoute("raven/{*id}")]
		public HttpResponseMessage RavenUiGet(string id = null)
		{
			if (string.IsNullOrEmpty(Database.Configuration.RedirectStudioUrl) == false)
			{
				var result = InnerRequest.CreateResponse(HttpStatusCode.Found);
				result.Headers.Location = new Uri(Path.Combine(DatabasesLandlord.SystemConfiguration.ServerUrl, Database.Configuration.RedirectStudioUrl));
				return result;
			}

			var docPath = GetRequestUrl().Replace("/raven/", "");
			return WriteEmbeddedFile(DatabasesLandlord.SystemConfiguration.WebDir, "Raven.Database.Server.WebUI", null, docPath);
		}

		[HttpGet]
		[RavenRoute("studio")]
		[RavenRoute("studio/{*path}")]
		public HttpResponseMessage GetStudioFile(string path = null)
		{
			if (string.IsNullOrEmpty(Database.Configuration.RedirectStudioUrl) == false)
			{
				var result = InnerRequest.CreateResponse(HttpStatusCode.Found);
				result.Headers.Location = new Uri(Path.Combine(DatabasesLandlord.SystemConfiguration.ServerUrl, Database.Configuration.RedirectStudioUrl));
				return result;
			}

			var url = GetRequestUrl();
			var docPath = url.StartsWith("/studio/") ? url.Substring("/studio/".Length) : url;
			return WriteEmbeddedFile("~/Server/Html5Studio", "Raven.Database.Server.Html5Studio", "Raven.Studio.Html5", docPath);
		}
	}
}