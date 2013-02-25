using System;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Security.OAuth
{
	public class OAuthCookie : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/OAuth/Cookie$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var auth = context.Request.Headers["Authorization"];
			if(string.IsNullOrEmpty(auth))
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Could not find authorization header"
				});
				return;
			}

			if (auth.StartsWith("Bearer") == false)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Authorization header does not starts with Bearer"
				});
				return;
			}

			context.Response.SetCookie("OAuth-Token", Uri.EscapeDataString(auth));
		}
	}
}