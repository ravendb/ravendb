using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Nancy;
using Nancy.ModelBinding;
using Raven.Client;
using Raven.Client.Connection.Async;
using Raven.ClusterManager.Models;
using Raven.ClusterManager.Tasks;
using HttpStatusCode = System.Net.HttpStatusCode;
using Raven.Abstractions.Extensions;

namespace Raven.ClusterManager.Modules
{
	public class CredentialsModule : NancyModule
	{
		private readonly IDocumentSession session;

		public CredentialsModule(IDocumentSession session)
			: base("/api/servers/credentials")
		{
			this.session = session;

			Post["/test"] = parameters => TestCredentials(Request.Query.ServerId);

			Post["/save"] = parameters => SaveCredentials(Request.Query.ServerId);
		}

		private async Task<object> SaveCredentials(string serverId)
		{
			var serverRecord = session.Load<ServerRecord>(serverId);
			if (serverRecord == null)
				return new NotFoundResponse();

			var credentials = this.Bind<ServerCredentials>();
			session.Store(credentials);
			serverRecord.CredentialsId = credentials.Id;

			await HealthMonitorTask.FetchServerDatabasesAsync(serverRecord, session);
			return null;
		}

		private async Task<object> TestCredentials(string serverId)
		{
			var serverRecord = session.Load<ServerRecord>(serverId);
			if (serverRecord == null)
				return new NotFoundResponse();

			var credentials = this.Bind<ServerCredentials>();

			var client = ServerHelpers.CreateAsyncServerClient(session, serverRecord, credentials);


			try
			{
				var adminStatistics = await client.Admin.GetStatisticsAsync();
			}
			catch (AggregateException ex)
			{
				var exception = ex.ExtractSingleInnerException();

				var webException = exception as WebException;
				if (webException != null)
				{
					var response = webException.Response as HttpWebResponse;
					if (response != null && response.StatusCode == HttpStatusCode.Unauthorized)
					{
						var failMessage = "Unauthorized. ";
						if (credentials.AuthenticationMode == AuthenticationMode.ApiKey)
						{
							failMessage += " Check that the Api Kay exists and enabled on the server.";
						}
						else
						{
							failMessage += " Check that the username exists in the server (or domain) and the password is correct and was not expired.";
						}
						return new CredentialsTest
						{
							Success = false,
							Message = failMessage,
							Type = "Unauthorized",
							Exception = response.ToString(),
						};
					}
					else
					{
						return new CredentialsTest
						{
							Success = false,
							Message = "Not found. Check that the server is online and that you have access to it.",
							Type = "NotFound",
							Exception = exception.ToString(),
						};
					}
				}

				return new CredentialsTest
				{
					Success = false,
					Message = "An error occurred. See exception for more details.",
					Exception = exception.ToString(),
				};
			}
			catch (Exception ex)
			{
				return new CredentialsTest
				{
					Success = false,
					Message = "An error occurred.",
					Exception = ex.ToString(),
				};
			}

			return new CredentialsTest
			{
				Success = true,
			};
		}
	}

	public class CredentialsTest
	{
		public bool Success { get; set; }
		public string Message { get; set; }
		public string Type { get; set; }
		public string Exception { get; set; }
	}
}