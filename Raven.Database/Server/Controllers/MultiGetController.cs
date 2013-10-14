using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Dispatcher;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Controllers
{
	public class MultiGetController : RavenApiController
	{
		private static ThreadLocal<bool> recursive = new ThreadLocal<bool>(() => false);

		[HttpPost("multi_get")]
		[HttpPost("databases/{databaseName}/multi_get")]
		public async Task<HttpResponseMessage> MultiGet()
		{
			if (recursive.Value)
				throw new InvalidOperationException("Nested requests to multi_get are not supported");
			recursive.Value = true;
			try
			{
				var requests = await ReadJsonObjectAsync<GetRequest[]>();
				var results = new HttpResponseMessage[requests.Length];

				await ExecuteRequests(results, requests);

				return new HttpResponseMessage(HttpStatusCode.OK)
				{
					Content = new MultiGetContent(results)
				};
			}
			finally
			{
				recursive.Value = false;
			}
		}

		public class MultiGetContent : HttpContent
		{
			private readonly HttpResponseMessage[] results;

			public MultiGetContent(HttpResponseMessage[] results)
			{
				this.results = results;
			}

			protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				var streamWriter = new StreamWriter(stream);
				var writer = new JsonTextWriter(streamWriter);
				writer.WriteStartArray();

				foreach (var result in results)
				{
					if (result == null)
					{
						writer.WriteNull();
						continue;
					}
					writer.WriteStartObject();

					writer.WritePropertyName("Status");
					writer.WriteValue((int) result.StatusCode);

					writer.WritePropertyName("Headers");

					writer.WriteStartObject();

					foreach (var header in result.Headers.Concat(result.Content.Headers))
					{
						foreach (var val in header.Value)
						{
							writer.WritePropertyName(header.Key);
							writer.WriteValue(val);
						}
					}

					writer.WriteEndObject();

					writer.WritePropertyName("Result");

					var jsonContent = (JsonContent)result.Content;

					if(jsonContent.Token != null)
						jsonContent.Token.WriteTo(writer, Default.Converters);

					writer.WriteEndObject();
				}


				writer.WriteEndArray();
				writer.Flush();
			}


			protected override bool TryComputeLength(out long length)
			{
				length = 0;
				return false;
			}
		}

		private async Task ExecuteRequests(HttpResponseMessage[] results, GetRequest[] requests)
		{
			// Need to create this here to preserve any current TLS data that we have to copy
			if ("yes".Equals(GetQueryStringValue("parallel"), StringComparison.OrdinalIgnoreCase))
			{
				var tasks = new Task[requests.Length];
				Parallel.For(0, requests.Length, position =>
					tasks[position] = HandleRequestAsync(requests, results, position)
					);
				await Task.WhenAll(tasks);
			}
			else
			{
				for (var i = 0; i < requests.Length; i++)
				{
					await HandleRequestAsync(requests, results, i);
				}
			}
		}

		private async Task HandleRequestAsync(GetRequest[] requests, HttpResponseMessage[] results, int i)
		{
			var request = requests[i];
			if (request == null)
				return;

			results[i] = await HandleActualRequestAsync(request);
		}

		private async Task<HttpResponseMessage> HandleActualRequestAsync(GetRequest request)
		{
			var msg = new HttpRequestMessage(HttpMethod.Get, new UriBuilder
			{
				Host = "multi.get",
				Query = request.Query,
				Path = request.Url
			}.Uri);
			msg.SetConfiguration(Configuration);
			var route = Configuration.Routes.GetRouteData(msg);
			msg.SetRouteData(route);
			var controllerSelector = new DefaultHttpControllerSelector(Configuration);
			var descriptor = controllerSelector.SelectController(msg);

			foreach (var header in request.Headers)
			{
				msg.Headers.TryAddWithoutValidation(header.Key, header.Value);
			}

			msg.Headers.TryAddWithoutValidation("Raven-internal-request", "true");

			var controller = (ApiController)descriptor.CreateController(msg);
			controller.Configuration = Configuration;
			var controllerContext = new HttpControllerContext(Configuration, route, msg)
			{
				ControllerDescriptor = descriptor,
				Controller = controller,
				RequestContext = new HttpRequestContext(),
				RouteData = route
			};
			controller.ControllerContext = controllerContext;
			controllerContext.Request = msg;
			controller.RequestContext = controllerContext.RequestContext;
			controller.Configuration = Configuration;

			return await controller.ExecuteAsync(controllerContext, CancellationToken.None);
		}
	}
}