using System;
using System.Linq;
using System.Net.Http;
using Nancy;
using Nancy.ModelBinding;
using Raven.Client;
using Raven.ClusterManager.Models;

namespace Raven.ClusterManager.Modules
{
	public class ServersModule : NancyModule
	{
		public ServersModule(IDocumentSession session)
			: base("/api/servers")
		{
			Get[""] = parameters =>
			{
				var servers = session.Query<ServerRecord>()
					.OrderByDescending(record => record.IsOnline)
					.ThenByDescending(record => record.LastOnlineTime)
					.Take(1024)
					.ToList();

				return new ClusterStatistics
				{
					Servers = servers,
				};
			};

			Post["/test-credentials"] = parameters =>
			{
				var input = this.Bind<ServerRecord>();

				var serverRecord = session.Load<ServerRecord>(input.Id);
				if (serverRecord == null)
					return new NotFoundResponse();

				var handler = new WebRequestHandler();
				var httpClient = new HttpClient(handler);
				try
				{
					// var result = await httpClient.GetAsync(serverRecord.Url + "admin/stats");
					throw new NotSupportedException("Waiting for the nancyfx async support");
					return true;
				}
				catch (HttpRequestException ex)
				{
					// Handle authentication.
				}

				return false;
			};

			Post["/save-credentials"] = parameters =>
			{
				var input = this.Bind<ServerRecord>();

				var serverRecord = session.Load<ServerRecord>(input.Id);
				if (serverRecord == null)
					return new NotFoundResponse();

				serverRecord.AuthenticationMode = input.AuthenticationMode;
				serverRecord.ApiKey = input.ApiKey;
				serverRecord.Username = input.Username;
				serverRecord.Password = input.Password;
				serverRecord.Domain = input.Domain;

				return null;
			};

			Delete["/{id}"] = parameters =>
			{
				var id = (string)parameters.id;
				session.Advanced.DocumentStore.DatabaseCommands.Delete(id, null);
				return true;
			};
		}
	}
}