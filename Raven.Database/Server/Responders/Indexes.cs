//-----------------------------------------------------------------------
// <copyright file="Indexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Indexes : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/indexes/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var namesOnlyString = context.Request.QueryString["namesOnly"];
			bool namesOnly;
			RavenJArray indexes;
			if(bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
				indexes = Database.GetIndexNames(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
			else
				indexes = Database.GetIndexes(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
			context.WriteJson(indexes);
		}
	}
}
