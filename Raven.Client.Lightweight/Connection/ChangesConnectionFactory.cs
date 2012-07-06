// -----------------------------------------------------------------------
//  <copyright file="ChangesConnectionFactory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Net;
using System.Text;
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
	public class ChangesConnectionFactory
	{
		private readonly DocumentConvention conventions;
		public event Action<Imports.SignalR.Client.Connection> ConfigureConnection = delegate { };

		public ChangesConnectionFactory(DocumentConvention conventions)
		{
			this.conventions = conventions;
		}

		public IObservable<ChangeNotification> Create(string url, ICredentials credentials, IDictionary<string, string> queryString)
		{
			var result = new FutureObservable<ChangeNotification>();
			EstablishConnection(url, credentials, queryString, 0)
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

		private Task<Imports.SignalR.Client.Connection> EstablishConnection(string url, ICredentials credentials, 
			IDictionary<string,string> queryString, int retries)
		{

			var connection = new Imports.SignalR.Client.Connection(url, queryString)
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
							return EstablishConnection(url, credentials,queryString, retries + 1);
						})
						.Unwrap();
				}).Unwrap();
		}
	}
}