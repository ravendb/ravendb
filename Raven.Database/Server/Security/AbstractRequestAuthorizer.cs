using System;
using System.Collections.Generic;
using System.Security.Principal;
using Raven.Database.Config;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Server.Security
{
	public abstract class AbstractRequestAuthorizer : IDisposable
	{
		[CLSCompliant(false)]
		protected InMemoryRavenConfiguration settings;
		[CLSCompliant(false)]
		protected DocumentDatabase database;
		[CLSCompliant(false)]
		protected IRavenServer server;
		[CLSCompliant(false)]
		protected Func<string> tenantId;

		public DocumentDatabase Database { get { return database; } }
		public InMemoryRavenConfiguration Settings { get { return settings; } }
		public string TenantId { get { return tenantId(); } }

		public void Initialize(DocumentDatabase database, InMemoryRavenConfiguration settings, Func<string> tenantIdGetter, IRavenServer theServer)
		{
			server = theServer;
			this.database = database;
			this.settings = settings;
			this.tenantId = tenantIdGetter;

			Initialize();
		}

		public void Initialize(DocumentDatabase database, IRavenServer theServer)
		{
			this.database = database;
			this.settings = database.Configuration;
			this.server = theServer;

			Initialize();
		}

		protected virtual void Initialize()
		{
		}

		public static bool IsGetRequest(string httpMethod, string requestPath)
		{
			return (httpMethod == "GET" || httpMethod == "HEAD") ||
				   httpMethod == "POST" && (requestPath == "/multi_get/" || requestPath == "/multi_get");
		}

		public abstract void Dispose();
	}
}