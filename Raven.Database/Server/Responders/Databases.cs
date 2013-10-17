using System;
using System.Collections.Generic;
using System.Security.Principal;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using System.Linq;
using Raven.Abstractions.Extensions;

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
			if (EnsureSystemDatabase(context) == false)
				return;

			// This responder is NOT secured, and anyone can access it.
			// Because of that, we need to provide explicit security here.
			
			// Anonymous Access - All / Get / Admin
			// Show all dbs

			// Anonymous Access - None
			// Show only the db that you have access to (read / read-write / admin)

			// If admin, show all dbs

			List<string> approvedDatabases = null;
			var databases = Database.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, context.GetStart(),
																		context.GetPageSize(Database.Configuration.MaxPageSize));
			var data = databases
				.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
				.ToArray();

			if (server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
			{
				var user = server.RequestAuthorizer.GetUser(context);
				if(user == null)
				{
					return;
				}

				if (user.IsAdministrator(server.SystemConfiguration.AnonymousUserAccessMode) == false)
				{
					approvedDatabases = server.RequestAuthorizer.GetApprovedDatabases(user, context, data);
				}
			}

			Etag lastDocEtag = Etag.Empty;
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

				if(approvedDatabases != null)
				{
					data = data.Where(s => approvedDatabases.Contains(s)).ToArray();
				}

				context.WriteJson(data);
			}
		}
	}
}