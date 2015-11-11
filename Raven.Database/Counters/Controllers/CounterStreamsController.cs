using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Streaming;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
	public class CounterStreamsController : ClusterAwareCountersApiController
	{
		[RavenRoute("cs/{counterStorageName}/streams/summaries")]
		[HttpGet]
		public HttpResponseMessage StreamCounterSummaries(string group)
		{
			var start = GetStart();
			var pageSize = GetPageSize(int.MaxValue);
			var writer = GetQueryStringValue("format");			

			HttpResponseMessage errorResponse;
			if (!ValidateStreamFormat(writer, out errorResponse))
				return errorResponse;

			if (string.IsNullOrEmpty(GetQueryStringValue("pageSize")))
				pageSize = int.MaxValue;

			Func<Stream,IOutputWriter> getWriter = stream =>
				writer.Equals("json", StringComparison.InvariantCultureIgnoreCase) ?
					(IOutputWriter)new JsonOutputWriter(stream) :
					new ExcelOutputWriter(stream);

			var msg = GetEmptyMessage();
						
			CounterStorage.MetricsCounters.ClientRequests.Mark();
			try
			{
				var reader = CounterStorage.CreateReader();
				group = group ?? string.Empty;
				var counters = reader.GetCounterSummariesByGroup(group, start, pageSize);
				msg.Content =
					new StreamContent(CountersLandlord,
						getWriter, counters.Select(RavenJObject.FromObject),
						mediaType => msg.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) { CharSet = "utf-8" },
						reader);
				
                if (IsCsvDownloadRequest(InnerRequest))
					msg.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");

				return msg;
			}
			catch (OperationCanceledException e)
			{
				throw new TimeoutException($"The query did not produce results in {DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout}", e);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private bool ValidateStreamFormat(string writer, out HttpResponseMessage response)
		{
			response = null;
			if (writer.Equals("json", StringComparison.InvariantCultureIgnoreCase) || 
				writer.Equals("excel", StringComparison.InvariantCultureIgnoreCase))
				return true;
			
			response = GetMessageWithObject(new
			{
				Message = "format parameter is required and must have either 'json' or 'excel' values"
			}, HttpStatusCode.BadRequest);
			return false;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static bool IsCsvDownloadRequest(HttpRequestMessage request)
		{
			return "true".Equals(GetQueryStringValue(request, "download"), StringComparison.InvariantCultureIgnoreCase)
				&& "excel".Equals(GetQueryStringValue(request, "format"), StringComparison.InvariantCultureIgnoreCase);
		}

		public class StreamContent : HttpContent
		{
			private readonly CountersLandlord landlord;
			private readonly Func<Stream,IOutputWriter> getWriter;
			private readonly IEnumerable<RavenJObject> content;
			private readonly Action<string> outputContentTypeSetter;
			private readonly CounterStorage.Reader reader;
			private readonly Lazy<NameValueCollection> headers;
			private readonly IPrincipal user;

			[CLSCompliant(false)]
			public StreamContent(CountersLandlord landlord,
				 Func<Stream, IOutputWriter> getWriter, 
				 IEnumerable<RavenJObject> content, 
				 Action<string> contentTypeSetter, 
				CounterStorage.Reader reader)
			{
				headers = CurrentOperationContext.Headers.Value;
				user = CurrentOperationContext.User.Value;
				this.landlord = landlord;
				this.getWriter = getWriter;
				this.content = content;
				outputContentTypeSetter = contentTypeSetter;
				this.reader = reader;
			}

			protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
			{
				var old = CurrentOperationContext.Headers.Value;
				var oldUser = CurrentOperationContext.User.Value;
				try
				{
					CurrentOperationContext.User.Value = user;
					CurrentOperationContext.Headers.Value = headers;
					CurrentOperationContext.RequestDisposables.Value.Add(reader);

                    using (var bufferedStream = new BufferedStream(stream, 1024 * 8))
					using (var cts = new CancellationTokenSource())
					using (var timeout = cts.TimeoutAfter(landlord.SystemConfiguration.DatabaseOperationTimeout))
					using (var writer = getWriter(bufferedStream))
					{
						outputContentTypeSetter(writer.ContentType);

						writer.WriteHeader();
						try
						{
							content.ForEach(item =>
							{
								timeout.Delay();
								writer.Write(item);
							});

							writer.Flush();
						}
						catch (Exception e)
						{
							writer.WriteError(e);
						}
						finally
						{
							bufferedStream.Flush();
                        }
					}
					return Task.FromResult(true);
				}
				finally
				{
					CurrentOperationContext.Headers.Value = old;
					CurrentOperationContext.User.Value = oldUser;
				}
			}			

			protected override bool TryComputeLength(out long length)
			{
				length = -1;
				return false;
			}
		}
	}
}
