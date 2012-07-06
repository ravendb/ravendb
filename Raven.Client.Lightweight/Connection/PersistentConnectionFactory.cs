// -----------------------------------------------------------------------
//  <copyright file="PersistentConnectionFactory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Util;
using Raven.Imports.SignalR.Client;
using Raven.Json.Linq;
using Raven.Client.Extensions;

namespace Raven.Client.Connection
{
	public class PersistentConnectionFactory
	{
		private readonly DocumentConvention conventions;
		public event Action<Imports.SignalR.Client.Connection> ConfigureConnection = delegate { };

		public event Action HandleUnauthorizedResponse = delegate { };

		public PersistentConnectionFactory(DocumentConvention conventions)
		{
			this.conventions = conventions;
		}

		public IObservable<ChangeNotification> Create(string url, ICredentials credentials)
		{
			var result = new FutureObservable<ChangeNotification>();
			EstablishConnection(url, credentials, 0)
				.ContinueWith(task =>
				{
					if(task.IsFaulted)
					{
						result.ForceError(task.Exception);
						return;
					}
					result.Add(task.Result.AsObservable<ChangeNotification>());
				});
			return result;
		}

		public Task HandleUnauthorizedResponseAsync(HttpWebResponse unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return null;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);

			if (unauthorizedResponseAsync == null)
				return null;

			return unauthorizedResponseAsync;
		}

		private Task<Imports.SignalR.Client.Connection> EstablishConnection(string url, ICredentials credentials, int retries)
		{
			var connection = new Imports.SignalR.Client.Connection(url)
			{
				Credentials = credentials
			};
			ConfigureConnection(connection);

			return connection.Start()
				.ContinueWith(task =>
				{
					task.AssertNotFailed();
					return connection;
				})
				.ContinueWith(task =>
				{
					var webException = task.Exception.ExtractSingleInnerException() as WebException;
					if (webException == null || retries >= 3)
						return task;// effectively throw

					var httpWebResponse = webException.Response as HttpWebResponse;
					if (httpWebResponse == null ||
						httpWebResponse.StatusCode != HttpStatusCode.Unauthorized)
						return task; // effectively throw

					var authorizeResponse = HandleUnauthorizedResponseAsync(httpWebResponse);

					if (authorizeResponse == null)
						return task; // effectively throw

					return authorizeResponse
						.ContinueWith(_ =>
						{
							_.Wait(); //throw on error
							return EstablishConnection(url, credentials, retries + 1);
						})
						.Unwrap();
				}).Unwrap();
		}
	}
}