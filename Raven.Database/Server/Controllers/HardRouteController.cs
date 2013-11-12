using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class HardRouteController : RavenApiController
	{
		[HttpGet][Route("favicon.ico")]
		public HttpResponseMessage FaviconGet()
		{
			return WriteEmbeddedFile(DatabasesLandlord.SystemConfiguration.WebDir, "favicon.ico");
		}

		[HttpGet][Route("clientaccesspolicy.xml")]
		public HttpResponseMessage ClientaccessPolicyGet()
		{
			var msg = new HttpResponseMessage
			{
				Content = new StringContent(@"<?xml version='1.0' encoding='utf-8'?>
<access-policy>
 <cross-domain-access>
   <policy>
	 <allow-from http-methods='*' http-request-headers='*'>
	   <domain uri='*' />
	 </allow-from>
	 <grant-to>
	   <resource include-subpaths='true' path='/' />
	 </grant-to>
   </policy>
 </cross-domain-access>
</access-policy>")

			};
			WriteETag(typeof(HardRouteController).FullName, msg);
			msg.Content.Headers.ContentType = new MediaTypeHeaderValue("text/xml");
			return msg;
		}

		public const string RootPath = "raven/studio.html";

		[HttpGet][Route("")]
		public HttpResponseMessage RavenRoot()
		{
			var location = DatabasesLandlord.SystemConfiguration.VirtualDirectory != "/" 
				? Path.Combine(DatabasesLandlord.SystemConfiguration.VirtualDirectory, RootPath) : RootPath;

			var result = InnerRequest.CreateResponse(HttpStatusCode.Found);
			result.Headers.Location = new Uri(Path.Combine(DatabasesLandlord.SystemConfiguration.ServerUrl, location));

			return result;
		}
	}
}
