//-----------------------------------------------------------------------
// <copyright file="Favicon.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Favicon : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/favicon.ico$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteEmbeddedFile(Settings.WebDir, "favicon.ico");
		}

		public override bool IsUserInterfaceRequest
		{
			get { return true; }
		}
	}
}