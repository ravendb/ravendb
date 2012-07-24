using System;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Databases : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/databases/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			switch (context.Request.HttpMethod)
			{
				case "GET":
					Guid lastDocEtag = Guid.Empty;
					Database.TransactionalStorage.Batch(accessor =>
					{
						lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
					});

					if (context.MatchEtag(lastDocEtag))
					{
						context.SetStatusToNotModified();
					}
					else
					{
						context.WriteHeaders(new RavenJObject(), lastDocEtag);

						context.WriteJson(Database.GetDocumentsWithIdStartingWith("Raven/Databases/", null, context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize)));
					}
					break;
			}
		}
	}
}