// -----------------------------------------------------------------------
//  <copyright file="DebugConfig.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders.Debugging
{
	public class DebugConfig : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/config$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}
		public override void Respond(IHttpContext context)
		{
			var cfg = RavenJObject.FromObject(Database.Configuration);
			cfg["OAuthTokenKey"] = "<not shown>";
			context.WriteJson(cfg);
		}
	}
}