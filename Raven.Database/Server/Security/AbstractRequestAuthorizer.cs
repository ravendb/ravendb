using System;
using System.Net;
using Raven.Database.Config;
using Raven.Database.Server.Controllers;
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

		public static bool IsGetRequest(RavenBaseApiController controller)
		{
            switch (controller.InnerRequest.Method.Method)
		    {
                case "GET":
                case "HEAD":
		            return true;
                case "POST":
		            var absolutePath = controller.InnerRequest.RequestUri.AbsolutePath;
				    return absolutePath.EndsWith("/queries", StringComparison.Ordinal) ||
				           absolutePath.EndsWith("/multi_get", StringComparison.Ordinal) ||
				           absolutePath.EndsWith("/multi_get/", StringComparison.Ordinal);
                default:
		            return false;
		    }
		}

		public abstract void Dispose();

	    public static bool IsGetRequest(HttpListenerRequest request)
	    {
            switch (request.HttpMethod)
            {
                case "GET":
                case "HEAD":
                    return true;
                case "POST":
                    var absolutePath = request.Url.AbsolutePath;
                    return (absolutePath.EndsWith("/multi_get", StringComparison.Ordinal) ||
                            absolutePath.EndsWith("/multi_get/", StringComparison.Ordinal));
                default:
                    return false;
            }
	    }
	}
}