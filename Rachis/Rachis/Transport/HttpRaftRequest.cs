//-----------------------------------------------------------------------
// <copyright file="HttpRaftRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Rachis.Transport
{
	/// <summary>
	/// A representation of an HTTP request to the Raft server
	/// </summary>
	public class HttpRaftRequest : IDisposable
	{
		internal readonly string Url;
		internal readonly string Method;
		private readonly Func<NodeConnectionInfo, Tuple<IDisposable, HttpClient>> getConnection;
		public HttpClient HttpClient { get; private set; }
		private IDisposable returnToQueue;

		private readonly NodeConnectionInfo nodeConnection;
		private bool isRequestSentToServer;

		public Func<HttpResponseMessage, NodeConnectionInfo, Task<Action<HttpClient>>> UnauthorizedResponseAsyncHandler { get; set; }

		public Func<HttpResponseMessage, NodeConnectionInfo, Task<Action<HttpClient>>> ForbiddenResponseAsyncHandler { get; set; }

		public HttpRaftRequest(NodeConnectionInfo nodeConnection, string url, string method, Func<NodeConnectionInfo, Tuple<IDisposable, HttpClient>> getConnection)
		{
			Url = url;
			Method = method;
			this.getConnection = getConnection;
			this.nodeConnection = nodeConnection;

			var connection = getConnection(nodeConnection);
			returnToQueue = connection.Item1;
			HttpClient = connection.Item2;
		}

		private Task SendRequestInternal(Func<HttpRequestMessage> getRequestMessage)
		{
			if (isRequestSentToServer && Debugger.IsAttached == false)
				throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");
			isRequestSentToServer = true;

			return RunWithAuthRetry(async () =>
			{
				var requestMessage = getRequestMessage();
				Response = await HttpClient.SendAsync(requestMessage).ConfigureAwait(false);

				return CheckForAuthErrors();
			});
		}

		private async Task RunWithAuthRetry(Func<Task<Boolean>> requestOperation)
		{
			int retries = 0;
			while (true)
			{
				if (await requestOperation().ConfigureAwait(false))
					return;

				if (++retries >= 3)
					return;

				if (Response.StatusCode == HttpStatusCode.Forbidden)
				{
					await HandleForbiddenResponseAsync(Response).ConfigureAwait(false);
					return;
				}

				if (await HandleUnauthorizedResponseAsync(Response).ConfigureAwait(false) == false)
					return;
			}
		}

		private bool CheckForAuthErrors()
		{
			return Response.StatusCode != HttpStatusCode.Unauthorized 
				&& Response.StatusCode != HttpStatusCode.Forbidden 
				&& Response.StatusCode != HttpStatusCode.PreconditionFailed;
		}

		public async Task<HttpResponseMessage> ExecuteAsync()
		{
			await SendRequestInternal(() => new HttpRequestMessage(new HttpMethod(Method), Url));

			return Response;
		}

		public async Task<HttpResponseMessage> WriteAsync(Func<HttpContent> content)
		{
			await SendRequestInternal(() => new HttpRequestMessage(new HttpMethod(Method), Url)
			{
				Content = content()
			});

			return Response;
		}

		public async Task<bool> HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse)
		{
			if (UnauthorizedResponseAsyncHandler == null)
				return false;

			var unauthorizedResponseAsync = UnauthorizedResponseAsyncHandler(unauthorizedResponse, nodeConnection);
			if (unauthorizedResponseAsync == null)
				return false;

			var configureHttpClient = await unauthorizedResponseAsync.ConfigureAwait(false);
			RecreateHttpClient(configureHttpClient); 
			return true;
		}

		private async Task HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse)
		{
			if (ForbiddenResponseAsyncHandler == null)
				return;

			var forbiddenResponseAsync = ForbiddenResponseAsyncHandler(forbiddenResponse, nodeConnection);
			if (forbiddenResponseAsync == null)
				return;

			await forbiddenResponseAsync.ConfigureAwait(false);
		}

		private void RecreateHttpClient(Action<HttpClient> configureHttpClient)
		{
			var connection = getConnection(nodeConnection);
			var newHttpClient = connection.Item2;
			var newReturnToQueue = connection.Item1;
			configureHttpClient(newHttpClient);

			DisposeInternal();

			HttpClient = newHttpClient;
			returnToQueue = newReturnToQueue;
			isRequestSentToServer = false;
		}

		public HttpResponseMessage Response { get; private set; }

		public void Dispose()
		{
			DisposeInternal();
		}

		private void DisposeInternal()
		{
			if (Response != null)
			{
				Response.Dispose();
				Response = null;
			}

			if (HttpClient != null)
			{
				if (returnToQueue != null)
					returnToQueue.Dispose();	

				HttpClient = null;
			}
		}
	}
}
