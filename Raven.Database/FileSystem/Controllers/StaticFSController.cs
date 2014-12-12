using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;

using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
	public class StaticFSController : RavenFsApiController
	{
		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/static/FavIcon")]		
		public HttpResponseMessage FavIcon()
		{
			return new HttpResponseMessage(HttpStatusCode.NotFound);
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/static/id")]
		public HttpResponseMessage Id()
		{
			return Request.CreateResponse(HttpStatusCode.OK, FileSystem.Storage.Id);
		}
	}
}