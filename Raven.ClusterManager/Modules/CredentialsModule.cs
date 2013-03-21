using System;
using System.Net;
using Nancy;
using Nancy.ModelBinding;
using Raven.Client;
using Raven.ClusterManager.Models;
using Raven.ClusterManager.Tasks;
using HttpStatusCode = System.Net.HttpStatusCode;
using Raven.Abstractions.Extensions;

namespace Raven.ClusterManager.Modules
{
	public class CredentialsModule : NancyModule
	{
		private readonly IAsyncDocumentSession session;

		public CredentialsModule(IAsyncDocumentSession session)
			: base("/api/servers/credentials")
		{
			this.session = session;

			Post["/save", true] = async (parameters, ct) =>
			{
				string serverId = Request.Query.ServerId;

				var serverRecord = await session.LoadAsync<ServerRecord>(serverId);
				if (serverRecord == null)
					return new NotFoundResponse();

				var credentials = this.Bind<ServerCredentials>();
				await session.StoreAsync(credentials);
				serverRecord.CredentialsId = credentials.Id;

				await HealthMonitorTask.FetchServerDatabasesAsync(serverRecord, session);
				return null;
			};

			Post["/test", true] = async (parameters, ct) =>
			{
				string serverId = Request.Query.ServerId;

				var serverRecord = await session.LoadAsync<ServerRecord>(serverId);
				if (serverRecord == null)
					return new NotFoundResponse();

				var credentials = this.Bind<ServerCredentials>();

				var client = await ServerHelpers.CreateAsyncServerClient(session, serverRecord, credentials);


				try
				{
					var adminStatistics = await client.GlobalAdmin.GetStatisticsAsync();
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