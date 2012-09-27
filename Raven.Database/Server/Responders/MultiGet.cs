using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Data;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class MultiGet : AbstractRequestResponder, IDisposable
	{
		public override string UrlPattern
		{
			get { return @"^/multi_get/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		private readonly ThreadLocal<bool> recursive = new ThreadLocal<bool>(() => false);

		public override void Respond(IHttpContext context)
		{
			if (recursive.Value)
				throw new InvalidOperationException("Nested requests to multi_get are not supported");
			recursive.Value = true;
			try
			{
				var requests = context.ReadJsonObject<GetRequest[]>();
				var results = new GetResponse[requests.Length];

				ExecuteRequests(context, Settings, results, requests);

				context.WriteJson(results);
			}
			finally
			{
				recursive.Value = false;
			}
		}

		private void ExecuteRequests(
			IHttpContext context,
			InMemoryRavenConfiguration ravenHttpConfiguration,
			GetResponse[] results,
			GetRequest[] requests)
		{
			// Need to create this here to preserve any current TLS data that we have to copy
			var contexts = requests.Select(request => new MultiGetHttpContext(ravenHttpConfiguration, context, request, TenantId))
				.ToArray();
			if ("yes".Equals(context.Request.QueryString["parallel"], StringComparison.InvariantCultureIgnoreCase))
			{
				Parallel.For(0, requests.Length, position =>
					HandleRequest(requests, results, position, context, ravenHttpConfiguration, contexts)
					);
			}
			else
			{
				for (var i = 0; i < requests.Length; i++)
				{
					HandleRequest(requests, results, i, context, ravenHttpConfiguration, contexts);
				}
			}
		}

		private void HandleRequest(GetRequest[] requests, GetResponse[] results, int i, IHttpContext context, InMemoryRavenConfiguration ravenHttpConfiguration, MultiGetHttpContext[] contexts)
		{
			var request = requests[i];
			if (request == null)
				return;
			server.HandleActualRequest(contexts[i]);
			results[i] = contexts[i].Complete();
		}

		public class MultiGetHttpContext : IHttpContext
		{
			private readonly InMemoryRavenConfiguration configuration;
			private readonly IHttpContext realContext;
			private readonly string tenantId;
			private readonly GetResponse getResponse;

			public MultiGetHttpContext(InMemoryRavenConfiguration configuration, IHttpContext realContext, GetRequest req, string tenantId)
			{
				this.configuration = configuration;
				this.realContext = realContext;
				this.tenantId = tenantId;
				getResponse = new GetResponse();
				if (req == null)
					return;
				Request = new MultiGetHttpRequest(req, realContext.Request);
				Response = new MultiGetHttpResponse(getResponse, realContext.Response);
			}

			public GetResponse Complete()
			{
				if (getResponse.Result != null)
					return getResponse;

				Response.OutputStream.Position = 0;
				getResponse.Result = RavenJToken.TryLoad(Response.OutputStream);
				getResponse.Status = Response.StatusCode != 0 ? Response.StatusCode : 200;
				return getResponse;
			}

			public bool RequiresAuthentication
			{
				get { return false; }
			}

			public InMemoryRavenConfiguration Configuration
			{
				get { return configuration; }
			}

			public IHttpRequest Request { get; set; }

			public IHttpResponse Response { get; set; }

			private IPrincipal currentUser;
			public IPrincipal User
			{
				get { return currentUser ?? realContext.User; }
				set { currentUser = value; }
			}


			public string GetRequestUrlForTenantSelection()
			{
				var requestUrl = this.GetRequestUrl();
				if (string.IsNullOrEmpty(tenantId) || tenantId == Constants.SystemDatabase)
					return requestUrl;
				return "/databases/" + tenantId + requestUrl;
			}

			public void FinalizeResonse()
			{
				// nothing here
			}

			public void SetResponseFilter(Func<Stream, Stream> responseFilter)
			{
				// nothing here
			}

			private readonly List<Action<ILog>> loggedMessages = new List<Action<ILog>>();
			public void OutputSavedLogItems(ILog logger)
			{
				foreach (var loggedMessage in loggedMessages)
				{
					loggedMessage(logger);
				}
			}

			public void Log(Action<ILog> loggingAction)
			{
				loggedMessages.Add(loggingAction);
			}

			public void SetRequestFilter(Func<Stream, Stream> requestFilter)
			{
				//nothing here
			}
		}

		public class MultiGetHttpRequest : IHttpRequest
		{
			public MultiGetHttpRequest(GetRequest req, IHttpRequest realRequest)
			{
				var tempQueryString = HttpUtility.ParseQueryString(req.Query ?? "");
				QueryString = new NameValueCollection();
				foreach (string key in tempQueryString)
				{
					var values = tempQueryString.GetValues(key);
					if (values == null)
						continue;
					foreach (var value in values)
					{
						QueryString.Add(key, HttpUtility.UrlDecode(value));
					}
				}
				Url = new UriBuilder(realRequest.Url)
				{
					Query = req.Query,
					Path = req.Url
				}.Uri;
				RawUrl = req.Url;
				IsLocal = realRequest.IsLocal;
				Headers = new NameValueCollection();
				foreach (var header in req.Headers)
				{
					Headers.Add(header.Key, header.Value);
				}
			}

			public bool IsLocal { get; set; }
			public NameValueCollection Headers { get; set; }

			public Stream InputStream
			{
				get { return Stream.Null; }
			}

			public NameValueCollection QueryString { get; set; }

			public string HttpMethod
			{
				get { return "GET"; }
			}

			public Uri Url
			{
				get;
				set;
			}

			public string RawUrl
			{
				get;
				set;
			}
		}

		public void Dispose()
		{
			recursive.Dispose();
		}

		public class MultiGetHttpResponse : IHttpResponse
		{
			private readonly GetResponse getResponse;

			public MultiGetHttpResponse(GetResponse getResponse, IHttpResponse response)
			{
				this.getResponse = getResponse;
				RedirectionPrefix = response.RedirectionPrefix;
				OutputStream = new MemoryStream();

			}

			public string RedirectionPrefix
			{
				get;
				set;
			}

			public void AddHeader(string name, string value)
			{
				getResponse.Headers[name] = value;
			}

			public Stream OutputStream { get; set; }

			public long ContentLength64
			{
				get;
				set;
			}

			public int StatusCode
			{
				get;
				set;
			}

			public string StatusDescription
			{
				get;
				set;
			}

			public string ContentType
			{
				get;
				set;
			}

			public void Redirect(string url)
			{
				getResponse.Status = 301;
				getResponse.Headers["Location"] = url;
			}

			public void Close()
			{
			}

			public void SetPublicCachability()
			{
				getResponse.Headers["Cache-Control"] = "Public";
			}

			public void WriteFile(string path)
			{
				using (var file = File.OpenRead(path))
				{
					file.CopyTo(OutputStream);
				}
			}

			public NameValueCollection GetHeaders()
			{
				throw new NotSupportedException();
			}

			public Task WriteAsync(string data)
			{
				throw new NotSupportedException();
			}
		}
	}

}