//-----------------------------------------------------------------------
// <copyright file="RavenRoot.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Http.Abstractions;

namespace Raven.Http.Responders
{
	public class RavenRoot : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/raven$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			if(Settings.VirtualDirectory != "/")
				context.Response.Redirect( Settings.VirtualDirectory + "/raven/index.html");
			else
				context.Response.Redirect("/raven/index.html");
		}

        public override bool IsUserInterfaceRequest
        {
            get
            {
                return true;
            }
        }
	}
}
