using Raven.Database.Commercial;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class Licensing : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/license/status$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteJson(ValidateLicense.CurrentLicense);	
		}
	}
}