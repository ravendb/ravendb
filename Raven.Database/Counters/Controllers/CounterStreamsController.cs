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
        [RavenRoute("cs/{counterStorageName}/streams/groups")]
        [HttpGet]
        public HttpResponseMessage StreamCounterGroups()
        {
            int skip;
            int take;
            HttpResponseMessage errorResponse;
            if (!GetAndValidateSkipAndTake(out skip, out take, out errorResponse))
                return errorResponse;

            Func<Stream, IOutputWriter> getWriter;
            if (!GetRelevantWriterFactory(out getWriter, out errorResponse))
                return errorResponse;

            var response = GetEmptyMessage();
            CounterStorage.MetricsCounters.ClientRequests.Mark();
            var reader = CounterStorage.CreateReader();
            var groups = reader.GetCounterGroups(skip, take); //since its enumerable, this is ok				
            response.Content =
                new StreamContent(CountersLandlord,
                    getWriter, groups.Select(RavenJObject.FromObject),
                    mediaType => response.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) {CharSet = "utf-8"},
                    reader);

            if (IsCsvDownloadRequest(InnerRequest))
                response.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");

            return response;
        }

        [RavenRoute("cs/{counterStorageName}/streams/summaries")]
        [HttpGet]
        public HttpResponseMessage StreamCounterSummaries(string group, string counterNamePrefix = null)
        {
            int skip;
            int take;
            HttpResponseMessage errorResponse;
            if (!GetAndValidateSkipAndTake(out skip, out take, out errorResponse)) 
                return errorResponse;

            Func<Stream, IOutputWriter> getWriter;
            if (!GetRelevantWriterFactory(out getWriter, out errorResponse)) 
                return errorResponse;

            var response = GetEmptyMessage();

            if (!string.IsNullOrWhiteSpace(@group))
            {
                return string.IsNullOrWhiteSpace(counterNamePrefix) ? 
                    GetSummariesPerGroupStreamResponse(@group, skip, take, response, getWriter) : 
                    GetSummariesPerGroupByPrefixStreamResponse(@group,counterNamePrefix, skip, take, response, getWriter);
            }

            return GetSummariesForAllGroupsStreamResponse(skip, take, response, getWriter);
        }

        private bool GetRelevantWriterFactory(out Func<Stream, IOutputWriter> getWriter, out HttpResponseMessage errorResponse)
        {
            var streamFormat = GetQueryStringValue("format");
            getWriter = null;
            if (!ValidateStreamFormat(streamFormat, out errorResponse))
                return false;

            getWriter = stream =>
                streamFormat.Equals("json", StringComparison.InvariantCultureIgnoreCase) ?
                    (IOutputWriter) new JsonOutputWriter(stream) :
                    new ExcelOutputWriter(stream);

            return true;
        }

        private bool GetAndValidateSkipAndTake(out int skip,out int take, out HttpResponseMessage httpResponseMessage)
        {
            httpResponseMessage = null;
            take = 0;
            var skipString = GetQueryStringValue("skip");
            skip = 0;
            if (string.IsNullOrEmpty(skipString) || !int.TryParse(skipString, out skip))
            {
                httpResponseMessage = GetMessageWithObject(new
                {
                    Message = "Failed to parse skip parameter - it should be of type 'int'."
                }, HttpStatusCode.BadRequest);
                return false;
            }
            skip = Math.Max(0, skip);

            var takeString = GetQueryStringValue("take");

            if (string.IsNullOrEmpty(takeString))
                take = int.MaxValue;
            else if (!int.TryParse(takeString, out take))
            {
                httpResponseMessage = GetMessageWithObject(new
                {
                    Message = "Failed to parse take parameter - it should be of type 'int'."
                }, HttpStatusCode.BadRequest);
                return false;
            }

            take = Math.Max(0, take);
            return true;
        }

        private HttpResponseMessage GetSummariesForAllGroupsStreamResponse(int skip, int take, HttpResponseMessage response, Func<Stream, IOutputWriter> getWriter)
        {
            CounterStorage.MetricsCounters.ClientRequests.Mark();
            var reader = CounterStorage.CreateReader();
            var groups = reader.GetCounterGroups(0, int.MaxValue); //since it is enumerable, this is not taking up alot of memory			
            var counters = groups.SelectMany(@group => reader.GetCounterSummariesByPrefix(@group.Name,null, skip, take));
            response.Content =
                new StreamContent(CountersLandlord,
                    getWriter, counters.Select(RavenJObject.FromObject),
                    mediaType => response.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) {CharSet = "utf-8"},
                    reader);

            if (IsCsvDownloadRequest(InnerRequest))
                response.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");

            return response;
        }

        private HttpResponseMessage GetSummariesPerGroupStreamResponse(string @group, int skip, int take, HttpResponseMessage response, Func<Stream, IOutputWriter> getWriter)
        {
            CounterStorage.MetricsCounters.ClientRequests.Mark();
            var reader = CounterStorage.CreateReader();
            @group = @group ?? string.Empty;
            var counters = reader.GetCounterSummariesByPrefix(@group,null, skip, take);
            response.Content =
                new StreamContent(CountersLandlord,
                    getWriter, counters.Select(RavenJObject.FromObject),
                    mediaType => response.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) {CharSet = "utf-8"},
                    reader);

            if (IsCsvDownloadRequest(InnerRequest))
                response.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");

            return response;
        }

        private HttpResponseMessage GetSummariesPerGroupByPrefixStreamResponse(string @group, string counterNamePrefix, int skip, int take, HttpResponseMessage response, Func<Stream, IOutputWriter> getWriter)
        {
            CounterStorage.MetricsCounters.ClientRequests.Mark();
            var reader = CounterStorage.CreateReader();
            @group = @group ?? string.Empty;
            var counters = reader.GetCounterSummariesByPrefix(@group,counterNamePrefix, skip, take);
            response.Content =
                new StreamContent(CountersLandlord,
                    getWriter, counters.Select(RavenJObject.FromObject),
                    mediaType => response.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType) { CharSet = "utf-8" },
                    reader);

            if (IsCsvDownloadRequest(InnerRequest))
                response.Content.Headers.Add("Content-Disposition", "attachment; filename=export.csv");

            return response;
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
            private const int StreamBufferSize = 1024 * 8;
            private readonly CountersLandlord landlord;
            private readonly Func<Stream, IOutputWriter> getWriter;
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

                    using (var bufferedStream = new BufferedStream(stream, StreamBufferSize))
                    using (var cts = new CancellationTokenSource())
                    using (var timeout = cts.TimeoutAfter(landlord.SystemConfiguration.DatabaseOperationTimeout))
                    using (var writer = getWriter(bufferedStream))
                    {
                        outputContentTypeSetter(writer.ContentType);
                        writer.WriteHeader();
                        try
                        {
                            foreach (var item in content)
                            {
                                timeout.ThrowIfCancellationRequested();
                                writer.Write(item);
                                timeout.Delay();
                            }
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
                catch (OperationCanceledException e)
                {
                    throw new TimeoutException($"Counters streaming operation timed-out in {landlord.SystemConfiguration.DatabaseOperationTimeout}", e);
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
