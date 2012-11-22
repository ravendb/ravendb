using System;
using System.Collections.Generic;
using Raven.Database.Config;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Security
{
	public abstract class AbstractRequestAuthorizer : IDisposable
	{
		[CLSCompliant(false)]
		protected Func<InMemoryRavenConfiguration> settings;
		[CLSCompliant(false)]
		protected Func<DocumentDatabase> database;
		[CLSCompliant(false)]
		protected HttpServer server;
		[CLSCompliant(false)]
		protected Func<string> tenantId;

		public DocumentDatabase ResourceStore { get { return database(); } }
		public InMemoryRavenConfiguration Settings { get { return settings(); } }
		public string TenantId { get { return tenantId(); } }

		public void Initialize(Func<DocumentDatabase> databaseGetter, Func<InMemoryRavenConfiguration> settingsGetter, Func<string> tenantIdGetter, HttpServer theServer)
		{
			server = theServer;
			this.database = databaseGetter;
			this.settings = settingsGetter;
			this.tenantId = tenantIdGetter;

			Initialize();
		}

		protected virtual void Initialize()
		{
		}

		public abstract bool Authorize(IHttpContext context);

		public static bool IsGetRequest(string httpMethod, string requestPath)
		{
			return (httpMethod == "GET" || httpMethod == "HEAD") ||
				   httpMethod == "POST" && (requestPath == "/multi_get/" || requestPath == "/multi_get");
		}

		public abstract List<string> GetApprovedDatabases(IHttpContext context);

		public abstract void Dispose();
	}
}