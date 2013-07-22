using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders.Debugging
{
	public class DebugSettings : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/settings$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}
		public override void Respond(IHttpContext context)
		{
			var key = context.Request.QueryString["key"];
			context.WriteJson(Database.Configuration.Settings[key]);
		}
	}
}
