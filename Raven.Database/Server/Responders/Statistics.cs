//-----------------------------------------------------------------------
// <copyright file="Statistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
	public class Statistics : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/stats$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteJson(Database.Statistics);
		}
	}
}
