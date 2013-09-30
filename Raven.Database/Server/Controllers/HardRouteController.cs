using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class HardRouteController : RavenApiController
	{
		[HttpGet("favicon.ico")]
		public HttpResponseMessage FaviconGet()
		{
			return WriteEmbeddedFile(DatabasesLandlord.SystemConfiguration.WebDir, "favicon.ico");
		}

		[HttpGet("clientaccesspolicy.xml")]
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
	}
}
