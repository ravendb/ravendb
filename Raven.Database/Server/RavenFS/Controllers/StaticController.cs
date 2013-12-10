using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class StaticController : RavenFsApiController
	{
		[HttpGet]
		public HttpResponseMessage ClientAccessPolicy()
		{
			var manifestResourceStream = typeof(StaticController).Assembly.GetManifestResourceStream("RavenFS.Static.ClientAccessPolicy.xml");

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = new StreamContent(manifestResourceStream)
				{
					Headers =
					{
						ContentType = new MediaTypeHeaderValue("text/xml")
					}
				}
			};
		}

		public Stream GetRavenStudioStream()
		{
			return (from path in RavenStudioPotentialPaths
					where File.Exists(path)
					select File.OpenRead(path)).FirstOrDefault();
		}

		private static IEnumerable<string> RavenStudioPotentialPaths
		{
			get
			{
				yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "RavenFS.Studio.xap");
#if DEBUG
				yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\RavenFS.Studio\Bin\Debug", "RavenFS.Studio.xap");
				yield return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\RavenFS.Studio\Bin\Debug", "RavenFS.Studio.xap");
#endif
			}
		}

		[HttpGet]
		public HttpResponseMessage RavenStudioXap()
		{
			var ravenStudioStream = GetRavenStudioStream();
			if (ravenStudioStream == null)
				return new HttpResponseMessage(HttpStatusCode.NotFound);

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = new StreamContent(ravenStudioStream)
				{
					Headers =
					{
						ContentType = new MediaTypeHeaderValue("application/x-silverlight-2")
					}
				}
			};
		}

		[HttpGet]
		public HttpResponseMessage FavIcon()
		{
			return new HttpResponseMessage(HttpStatusCode.NotFound);
		}

		[HttpGet]
		public HttpResponseMessage Root()
		{
			var file = RavenStudioPotentialPaths.Any(File.Exists) ? "RavenFS.Studio.html" : "studio_not_found.html";

			var manifestResourceStream = typeof(StaticController).Assembly.GetManifestResourceStream("RavenFS.Static." + file);

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = new StreamContent(manifestResourceStream)
				{
					Headers =
					{
						ContentType = new MediaTypeHeaderValue("text/html")
					}
				}
			};
		}

		[HttpGet]
		public HttpResponseMessage Id()
		{
			var ravenFileSystem = (RavenFileSystem)ControllerContext.Configuration.DependencyResolver.GetService(typeof(RavenFileSystem));

			return Request.CreateResponse(HttpStatusCode.OK, ravenFileSystem.Storage.Id);
		}
	}
}