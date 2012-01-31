//-----------------------------------------------------------------------
// <copyright file="RavenUI.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.IO;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class RavenUI : AbstractRequestResponder
	{
		private const string SilverlightXapName = "Raven.Studio.xap";

		public override string UrlPattern
		{
			get { return "^/raven/"; }
		}

		public override bool IsUserInterfaceRequest
		{
			get { return true; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			if (context.Request.Url.AbsolutePath == RavenRoot.RootPath && 
				SilverlightUI.GetPaths(SilverlightXapName, ResourceStore.Configuration.WebDir).All(f => File.Exists(f) == false))
			{
				context.WriteEmbeddedFile(Settings.WebDir, "studio_not_found.html");
				return;
			}
			var docPath = context.GetRequestUrl().Replace("/raven/", "");
			context.WriteEmbeddedFile(Settings.WebDir, docPath);
		}
	}
}