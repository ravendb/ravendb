using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Nancy;
using Nancy.ModelBinding;
using Raven.Client;
using Raven.ClusterManager.Models;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace Raven.ClusterManager.Modules
{
	public class CredentialsModule : NancyModule
	{
		private readonly IDocumentSession session;

		public CredentialsModule(IDocumentSession session)
			: base("/api/servers/credentials/{*id}")
		{
			this.session = session;

			Post["/test"] = parameters => TestCredentials(parameters);

			Post["/save"] = parameters =>
			{
				var input = this.Bind<ServerCredentials>();
				session.Store(input);
				return null;
			};
		}

		private async Task<object> TestCredentials(dynamic parameters)
		{
			var serverRecord = session.Load<ServerRecord>(parameters.Id);
			if (serverRecord == null)
				return new NotFoundResponse();

			var input = this.Bind<ServerCredentials>();

			var handler = new WebRequestHandler();
			handler.Credentials = new NetworkCredential(input.Username, input.Password);
			var httpClient = new HttpClient(handler);
			try
			{
				var result = await httpClient.GetAsync(serverRecord.Url + "admin/stats");
				if (result.StatusCode == HttpStatusCode.Unauthorized)
					return false;
				
				return true;
			}
			catch (HttpRequestException ex)
			{
				// Handle authentication.
			}

			return false;
		}
	}
}