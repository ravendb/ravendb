using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Json.Linq;

namespace Raven.Client.FileSystem.Extensions
{
    internal static class AsyncFilesServerClientExtension
    {
        internal static async Task<RavenJObject> GetMetadataForAsyncImpl(IHoldProfilingInformation self, HttpJsonRequestFactory requestFactory, FilesConvention conventions,
            NameValueCollection operationsHeaders, string filename, string baseUrl, OperationCredentials credentials)
        {
            using (var request = requestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(self, baseUrl + "/files?name=" + Uri.EscapeDataString(filename), HttpMethod.Head, credentials, conventions)).AddOperationHeaders(operationsHeaders))
            {
                try
                {
                    await request.ExecuteRequestAsync().ConfigureAwait(false);

                    var response = request.Response;

                    var metadata = response.HeadersToObject();
                    metadata[Constants.MetadataEtagField] = metadata[Constants.MetadataEtagField].Value<string>().Trim('\"');
                    return metadata;
                }
                catch (Exception e)
                {
                    try
                    {
                        throw e.SimplifyException();
                    }
                    catch (FileNotFoundException)
                    {
                        return null;
                    }
                }
            }
        }

        internal static async Task<Stream> DownloadAsyncImpl(IHoldProfilingInformation self, HttpJsonRequestFactory requestFactory, FilesConvention conventions,
            NameValueCollection operationsHeaders, string path, string filename, Reference<RavenJObject> metadataRef, long? @from, long? to, string baseUrl, OperationCredentials credentials)
        {
            var request = requestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(self, baseUrl + path + Uri.EscapeDataString(filename), HttpMethod.Get, credentials, conventions)).AddOperationHeaders(operationsHeaders);

            if (@from != null)
            {
                if (to != null)
                    request.AddRange(@from.Value, to.Value);
                else
                    request.AddRange(@from.Value);
            }

            HttpResponseMessage response = null;
            try
            {
                response = await request.ExecuteRawResponseAsync().ConfigureAwait(false);
                if (response.StatusCode == HttpStatusCode.NotFound)
                    throw new FileNotFoundException("The file requested does not exists on the file system.", baseUrl + path + filename);

                await response.AssertNotFailingResponse().ConfigureAwait(false);

                if (metadataRef != null)
                    metadataRef.Value = response.HeadersToObject();

                return new DisposableStream(await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false),() =>
                {
                    request.Dispose();
                    response.Content.Dispose();
                    response.Dispose();
                });
            }
            catch (Exception e)
            {
                try
                {
                    request.Dispose();

                    if (response != null)
                    {
                        response.Content.Dispose();
                        response.Dispose();
                    }
                }
                catch (Exception)
                {
                }

                throw e.SimplifyException();
            }
        }

        internal static void AddEtagHeader(HttpJsonRequest request, Etag etag)
        {
            if (etag != null)
            {
                request.AddHeader("If-None-Match", "\"" + etag + "\"");
            }
        }
    }
}
