//-----------------------------------------------------------------------
// <copyright file="RavenRoot.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class RavenRoot : AbstractRequestResponder
	{
		public const string RootPath = "/raven/studio.html";
		public override string UrlPattern
		{
			get { return "^/(raven)?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			if(Settings.VirtualDirectory != "/")
				context.Response.Redirect(Settings.VirtualDirectory + RootPath);
			else
				context.Response.Redirect(RootPath);
		}

		public override bool IsUserInterfaceRequest
		{
			get { return true; }
		}
	}
}