//-----------------------------------------------------------------------
// <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class BuildVersion : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/build/version$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteJson(new
			{
				DocumentDatabase.ProductVersion,
				DocumentDatabase.BuildVersion
			});
		}
	}
}