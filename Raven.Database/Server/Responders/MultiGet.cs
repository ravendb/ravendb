using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using NLog;
using Raven.Abstractions.Data;
using Raven.Http;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;
using System.Linq;

namespace Raven.Database.Server.Responders
{
	public class MultiGet : RequestResponder
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
				//Parallel.For(0, requests.Length, position => 
				//    HandleRequest(requests, results, position, context)
				//    );

				for (int i = 0; i < requests.Length; i++)
				{
					HandleRequest(requests, results, i, context);
				}
				context.WriteJson(results);
			}
			finally
			{
				recursive.Value = false;
			}
		}

		private void HandleRequest(GetRequest[] requests, GetResponse[] results, int i, IHttpContext context)
		{
			var request = requests[i];
			if (request == null)
				return;
			var ctx = new MultiGetHttpContext(Settings, context, request, TenantId);
			server.HandleActualRequest(ctx);
			results[i] = ctx.Complete();
		}

		public class MultiGetHttpContext : IHttpContext
		{
			private readonly IRavenHttpConfiguration configuration;
			private readonly IHttpContext realContext;
			private readonly string tenantId;
			private readonly GetResponse getResponse;

			public MultiGetHttpContext(IRavenHttpConfiguration configuration, IHttpContext realContext, GetRequest req, string tenantId)
			{
				this.configuration = configuration;
				this.realContext = realContext;
				this.tenantId = tenantId;
				getResponse = new GetResponse();
				Request = new MultiGetHttpRequest(req, realContext.Request);
				Response = new MultiGetHttpResponse(getResponse, realContext.Response);
			}

			public GetResponse Complete()
			{
				if(getResponse.Result!=null)
					return getResponse;

				Response.OutputStream.Position = 0;
				getResponse.Result = new StreamReader(Response.OutputStream).ReadToEnd();
				if (Response.StatusCode != 0)
					getResponse.Status = Response.StatusCode;
				else
					getResponse.Status = 200;
				return getResponse;
			}

			public IRavenHttpConfiguration Configuration
			{
				get { return configuration; }
			}

			public IHttpRequest Request { get; set; }

			public IHttpResponse Response { get; set; }

			public IPrincipal User
			{
				get { return realContext.User; }
			}


			public string GetRequestUrlForTenantSelection()
			{
				var requestUrl = this.GetRequestUrl();
				if (string.IsNullOrEmpty(tenantId) || tenantId == Constants.DefaultDatabase)
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

			public void OutputSavedLogItems(Logger logger)
			{
				realContext.OutputSavedLogItems(logger);
			}

			public void Log(Action<Logger> loggingAction)
			{
				realContext.Log(loggingAction);
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
					if(values == null)
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
				RawUrl = Url.ToString();
				Headers = new NameValueCollection();
				foreach (var header in req.Headers)
				{
					Headers.Add(header.Key, header.Value);
				}
			}

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
		}
	}

}