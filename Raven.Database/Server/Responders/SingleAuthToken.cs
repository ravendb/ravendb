// -----------------------------------------------------------------------
//  <copyright file="SingleAuthToken.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class SingleAuthToken : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/singleAuthToken$"; }
		}
		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			// using windows auth with anonymous access = none sometimes generate a 401 even though we made two requests
			// instead of relying on windows auth, which require request buffering, we generate a one time token and return it.
			// we KNOW that the user have access to this db for writing, since they got here, so there is no issue in generating 
			// a single use token for them.
			var token = server.RequestAuthorizer.GenerateSingleUseAuthToken(Database, context.User);
			context.WriteJson(new
			{
				Token = token
			});
		}
	}
}