//-----------------------------------------------------------------------
// <copyright file="RavenUI.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Http.Responders
{
	public class RavenUI : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/raven/"; }
		}

        public override bool IsUserInterfaceRequest
        {
            get
            {
                return true;
            }
        }

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var docPath = context.GetRequestUrl().Replace("/raven/", "");
			context.WriteEmbeddedFile(ResourceStore.GetType().Assembly,Settings.WebDir, docPath);
		}
	}
}
