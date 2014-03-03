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
	public class StaticFSController : RavenFsApiController
	{
		[HttpGet]
        [Route("ravenfs/{fileSystemName}/static/clientAccessPolicy")]		

		public HttpResponseMessage ClientAccessPolicy()
		{
			var manifestResourceStream = typeof(StaticFSController).Assembly.GetManifestResourceStream("RavenFS.Static.ClientAccessPolicy.xml");

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
        [Route("ravenfs/{fileSystemName}/static/RavenStudioXap")]		
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
        [Route("ravenfs/{fileSystemName}/static/FavIcon")]		
		public HttpResponseMessage FavIcon()
		{
			return new HttpResponseMessage(HttpStatusCode.NotFound);
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/static/Root")]		
		public HttpResponseMessage Root()
		{
			var file = RavenStudioPotentialPaths.Any(File.Exists) ? "RavenFS.Studio.html" : "studio_not_found.html";

			var manifestResourceStream = typeof(StaticFSController).Assembly.GetManifestResourceStream("RavenFS.Static." + file);

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
        [Route("ravenfs/{fileSystemName}/staticfs/id")]
		public HttpResponseMessage Id()
		{
			return Request.CreateResponse(HttpStatusCode.OK, RavenFileSystem.Storage.Id);
		}
	}
}