using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class ClientAccessPolicy : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/clientaccesspolicy.xml$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[]{"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteETag(typeof (ClientAccessPolicy).FullName);
			context.Response.SetPublicCachability();
			context.Response.ContentType = "text/xml";
			context.Write(@"<?xml version='1.0' encoding='utf-8'?>
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
</access-policy>");
		}
	}
}