using System;
using Raven.Database.Data;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Docs : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "/docs/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "POST"}; }
		}

		public override void Respond(IHttpContext context)
		{
			switch (context.Request.HttpMethod)
			{
				case "GET":
					context.WriteJson(Database.GetDocuments(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize), context.GetEtagFromQueryString()));
					break;
				case "POST":
					var json = context.ReadJson();
					var id = Database.Put(null, Guid.NewGuid(), json,
										  context.Request.Headers.FilterHeaders(isServerDocument: true),
                                          GetRequestTransaction(context));
					context.SetStatusToCreated("/docs/" + id);
					context.WriteJson(id);
					break;
			}
		}
	}
}