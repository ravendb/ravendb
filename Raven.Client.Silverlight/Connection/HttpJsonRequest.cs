//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Browser;
using System.Reflection;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Ionic.Zlib;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Client.Silverlight.MissingFromSilverlight;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Client.Extensions;
using Raven.Abstractions.Connection;

namespace Raven.Client.Silverlight.Connection
{
    /// <summary>
    /// A representation of an HTTP json request to the RavenDB server
    /// Since we are using the ClientHttp stack for Silverlight, we don't need to implement
    /// caching, it is already implemented for us.
    /// Note: that the RavenDB server generates both an ETag and an Expires header to ensure proper
    /// Note: behavior from the silverlight http stack
    /// </summary>
    public class HttpJsonRequest
    {
        private readonly string url;
        private readonly DocumentConvention conventions;
        internal volatile HttpWebRequest webRequest;
        private byte[] postedData;
        private int retries;
        public static readonly string ClientVersion = new AssemblyName(typeof(HttpJsonRequest).Assembly.FullName).Version.ToString();
        private bool disabledAuthRetries;

        private string primaryUrl;

        private string operationUrl;

        public Action<NameValueCollection, string, string> HandleReplicationStatusChanges = delegate { };

        public string ContentType
        {
            get { return webRequest.ContentType; }
            set { webRequest.ContentType = value; }
        }

        public WebHeaderCollection Headers
        {
            get { return webRequest.Headers; }
        }

        private Task RecreateWebRequest(Action<HttpWebRequest> result)
        {
            retries++;
            // we now need to clone the request, since just calling GetRequest again wouldn't do anything
            var newWebRequest = (HttpWebRequest)WebRequestCreator.ClientHttp.Create(new Uri(url));
            newWebRequest.Method = webRequest.Method;
            HttpJsonRequestHelper.CopyHeaders(webRequest, newWebRequest);
            newWebRequest.Credentials = webRequest.Credentials;
            result(newWebRequest);
            webRequest = newWebRequest;
            requestSendToServer = false;

            if (postedData == null)
            {
                var taskCompletionSource = new TaskCompletionSource<object>();
                taskCompletionSource.SetResult(null);
                return taskCompletionSource.Task;
            }
            else return WriteAsync(postedData);
        }

        public void DisableAuthentication()
        {
            webRequest.Credentials = null;
            webRequest.UseDefaultCredentials = false;
            disabledAuthRetries = true;
        }

        public void RemoveAuthorizationHeader()
        {
            var headersWithoutAuthorization = new WebHeaderCollection();

            foreach (var header in webRequest.Headers.AllKeys)
            {
                if (header == "Authorization")
                    continue;

                headersWithoutAuthorization[header] = webRequest.Headers[header];
            }

            webRequest.Headers = headersWithoutAuthorization;
        }

        private HttpJsonRequestFactory factory;

        private static Task noopWaitForTask = new CompletedTask();


        public TimeSpan Timeout
        {
            set { } // can't set timeout in Silverlight
        }
        /// <summary>
        /// Gets or sets the response headers.
        /// </summary>
        /// <value>The response headers.</value>
        public NameValueCollection ResponseHeaders { get; set; }

        internal HttpJsonRequest(CreateHttpJsonRequestParams requestParams, HttpJsonRequestFactory factory)
        {
            _credentials = requestParams.Credentials;
            this.url = requestParams.Url;
            this.conventions = requestParams.Convention;
            this.factory = factory;
            webRequest = (HttpWebRequest)WebRequestCreator.ClientHttp.Create(new Uri(url));
            noopWaitForTask = new CompletedTask();
            WaitForTask = noopWaitForTask;

            webRequest.Headers["Raven-Client-Version"] = ClientVersion;

            WriteMetadata(requestParams.Metadata);
            webRequest.Method = requestParams.Method;
            if (requestParams.Method != "GET")
                webRequest.ContentType = "application/json; charset=utf-8";

            if (factory.DisableRequestCompression == false && requestParams.DisableRequestCompression == false)
            {
                if (requestParams.Method == "POST" || requestParams.Method == "PUT" ||
                    requestParams.Method == "PATCH" || requestParams.Method == "EVAL")
                    webRequest.Headers["Content-Encoding"] = "gzip";
            }
        }

        public async Task<RavenJToken> ReadResponseJsonAsync()
        {
            var s = await ReadResponseStringAsync();
            return RavenJToken.Parse(s);
        }

        public Task<RavenJToken> ExecuteRequestAsync()
        {
            return ReadResponseJsonAsync();
        }

        private bool requestSendToServer;

        private readonly OperationCredentials _credentials;

        /// <summary>
        /// Begins the read response string.
        /// </summary>
        private async Task<string> ReadResponseStringAsync()
        {
            if (requestSendToServer)
                throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");

            requestSendToServer = true;
            Task authorizeResponse = null;
            await WaitForTask;
            try
            {
                var webResponse = await webRequest.GetResponseAsync();
                return ReadStringInternal(() => webResponse);
            }
            catch (SecurityException e)
            {
                throw new WebException(
                    "Could not contact server.\r\nGot security error because RavenDB wasn't able to contact the database to get ClientAccessPolicy.xml permission.",
                    e)
                {
                    Data = { { "Url", webRequest.RequestUri } }
                };
            }
            catch (WebException we)
            {
                if (we.Response == null)
                {
                    we.Data["Url"] = webRequest.RequestUri;
                    throw;
                }

                var webResponse = ((HttpWebResponse)we.Response);
                switch (webResponse.StatusCode)
                {
                    case HttpStatusCode.BadRequest:
                        var error = we.TryReadErrorResponseObject(new { Message = "" });
                        if (error != null && error.Message != null)
                        {
                            throw new BadRequestException(error.Message);
                        }
                        break;
                    case HttpStatusCode.Unauthorized:
                        authorizeResponse = HandleUnauthorizedResponseAsync(webResponse);
                        if (authorizeResponse == null)
                        {
                            throw;
                        }
                        break;
                    case HttpStatusCode.Forbidden:
                        HandleForbiddenResponseAsync(webResponse);
                        break;
                    case HttpStatusCode.PreconditionFailed:
                        break;
                }
                if (authorizeResponse == null)
                    throw;
            }
            catch (Exception e)
            {
                e.Data["Url"] = webRequest.RequestUri;
                throw;
            }
            await authorizeResponse;
            return await ReadResponseStringAsync();
        }

        private void HandleForbiddenResponseAsync(HttpWebResponse forbiddenResponse)
        {
            if (conventions.HandleForbiddenResponseAsync == null)
                return;

            conventions.HandleForbiddenResponseAsync(forbiddenResponse, _credentials);
        }

        public Task HandleUnauthorizedResponseAsync(HttpWebResponse unauthorizedResponse)
        {
            if (conventions.HandleUnauthorizedResponseAsync == null)
                return null;

            var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse, _credentials);

            if (unauthorizedResponseAsync == null)
                return null;

            return unauthorizedResponseAsync.ContinueWith(task => RecreateWebRequest(task.Result)).Unwrap();
        }

        public async Task<byte[]> ReadResponseBytesAsync()
        {
            await WaitForTask;
            Task authorizeResponse = null;
            try
            {
                var webResponse = await webRequest.GetResponseAsync();
                return ReadResponse(() => webResponse,ConvertStreamToBytes);
            }
            catch (SecurityException e)
            {
                throw new WebException(
                    "Could not contact server.\r\nGot security error because RavenDB wasn't able to contact the database to get ClientAccessPolicy.xml permission.",
                    e)
                {
                    Data = { { "Url", webRequest.RequestUri } }
                };
            }
            catch (WebException we)
            {
                if (we.Response == null)
                {
                    we.Data["Url"] = webRequest.RequestUri;
                    throw;
                }

                var webResponse = ((HttpWebResponse)we.Response);
                switch (webResponse.StatusCode)
                {
                    case HttpStatusCode.BadRequest:
                        var error = we.TryReadErrorResponseObject(new { Message = "" });
                        if (error != null && error.Message != null)
                        {
                            throw new BadRequestException(error.Message);
                        }
                        break;
                    case HttpStatusCode.Unauthorized:
                        authorizeResponse = HandleUnauthorizedResponseAsync(webResponse);
                        if (authorizeResponse == null)
                        {
                            throw;
                        }
                        break;
                    case HttpStatusCode.Forbidden:
                        HandleForbiddenResponseAsync(webResponse);
                        break;
                    case HttpStatusCode.PreconditionFailed:
                        break;
                }
                if (authorizeResponse == null)
                    throw;
            }
            catch (Exception e)
            {
                e.Data["Url"] = webRequest.RequestUri;
                throw;
            }
            await authorizeResponse;
            return await ReadResponseBytesAsync();
        }

        static byte[] ConvertStreamToBytes(Stream input)
        {
            var buffer = new byte[16 * 1024];
            using (var ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }

        private string ReadStringInternal(Func<WebResponse> getResponse)
        {
            return ReadResponse(getResponse, responseStream =>
                {
                    var reader = new StreamReader(responseStream);
                    var text = reader.ReadToEnd();
                    return text;
                }
            );

        }

        private T ReadResponse<T>(Func<WebResponse> getResponse, Func<Stream, T> handleResponse)
        {
            WebResponse response;
            try
            {
                response = getResponse();
            }
            catch (WebException e)
            {
                var httpWebResponse = e.Response as HttpWebResponse;
                if (httpWebResponse == null ||
                    httpWebResponse.StatusCode == HttpStatusCode.NotFound ||
                        httpWebResponse.StatusCode == HttpStatusCode.Conflict)
                    throw;

                using (var sr = new StreamReader(e.Response.GetResponseStream()))
                {
                    throw new InvalidOperationException(sr.ReadToEnd(), e);
                }
            }

            ResponseHeaders = new NameValueCollection();
            foreach (var key in response.Headers.AllKeys)
            {
                ResponseHeaders[key] = response.Headers[key];
            }

            ResponseStatusCode = ((HttpWebResponse)response).StatusCode;

            using (var responseStream = response.GetResponseStreamWithHttpDecompression())
            {
                return handleResponse(responseStream);
            }
        }


        /// <summary>
        /// Gets or sets the response status code.
        /// </summary>
        /// <value>The response status code.</value>
        public HttpStatusCode ResponseStatusCode { get; set; }

        /// <summary>
        /// The task to wait all other actions on
        /// </summary>
        public Task WaitForTask { get; set; }

        public string Method
        {
            get { return webRequest.Method; }
        }

        public string Url
        {
            get { return webRequest.RequestUri.ToString(); }
        }

        private void WriteMetadata(RavenJObject metadata)
        {
            if (metadata == null)
                return;

            foreach (var prop in metadata)
            {
                if (prop.Value == null)
                    continue;

                string value;
                switch (prop.Value.Type)
                {
                    case JTokenType.Array:
                        value = prop.Value.Value<RavenJArray>().ToString(Formatting.None);
                        break;
                    case JTokenType.Object:
                        value = prop.Value.Value<RavenJObject>().ToString(Formatting.None);
                        break;
                    default:
                        value = prop.Value.Value<object>().ToString();
                        break;
                }
                var headerName = prop.Key;
                if (headerName == "ETag")
                    headerName = "If-None-Match";
                if (headerName.StartsWith("@") ||
                    headerName == Constants.LastModified ||
                    headerName == Constants.RavenLastModified)
                    continue;
                switch (headerName)
                {
                    case "Content-Length":
                        break;
                    case "Content-Type":
                        webRequest.ContentType = value;
                        break;
                    default:
                        webRequest.Headers[headerName] = value;
                        break;
                }
            }
        }

        /// <summary>
        /// Begins the write operation
        /// </summary>
        public Task WriteAsync(string data)
        {
            return WaitForTask.ContinueWith(_ =>
                                            webRequest.GetRequestStreamAsync()
                                                .ContinueWith(task =>
                                                {
                                                    Stream dataStream = factory.DisableRequestCompression == false ?
                                                        new GZipStream(task.Result, CompressionMode.Compress) :
                                                        task.Result;
                                                    var streamWriter = new StreamWriter(dataStream, Encoding.UTF8);
                                                    return streamWriter.WriteAsync(data)
                                                        .ContinueWith(writeTask =>
                                                        {
                                                            streamWriter.Dispose();
                                                            dataStream.Dispose();
                                                            task.Result.Dispose();
                                                            return writeTask;
                                                        }).Unwrap();
                                                }).Unwrap())
                .Unwrap();
        }

        /// <summary>
        /// Begins the write operation
        /// </summary>
        public Task WriteAsync(byte[] byteArray)
        {
            postedData = byteArray;
            return WaitForTask.ContinueWith(_ => webRequest.GetRequestStreamAsync().ContinueWith(t =>
            {
                var dataStream = new GZipStream(t.Result, CompressionMode.Compress);
                using (dataStream)
                {
                    dataStream.Write(byteArray, 0, byteArray.Length);
                    dataStream.Close();
                }
            })).Unwrap();
        }

        /// <summary>
        /// Adds the operation headers.
        /// </summary>
        /// <param name="operationsHeaders">The operations headers.</param>
        public HttpJsonRequest AddOperationHeaders(IDictionary<string, string> operationsHeaders)
        {
            foreach (var header in operationsHeaders)
            {
                webRequest.Headers[header.Key] = header.Value;
            }
            return this;
        }

        /// <summary>
        /// Adds the operation header
        /// </summary>
        public HttpJsonRequest AddOperationHeader(string key, string value)
        {
            webRequest.Headers[key] = value;
            return this;
        }

        public Task<IObservable<string>> ServerPullAsync(int retries = 0)
        {
            return WaitForTask.ContinueWith(__ =>
            {
                webRequest.AllowReadStreamBuffering = false;
                webRequest.AllowWriteStreamBuffering = false;
                webRequest.Headers["Requires-Big-Initial-Download"] = "True";
                return webRequest.GetResponseAsync()
                   .ContinueWith(task =>
                   {
                       var stream = task.Result.GetResponseStream();
                       var observableLineStream = new ObservableLineStream(stream, () =>
                                                                                       {
                                                                                           webRequest.Abort();
                                                                                           try
                                                                                           {
                                                                                               task.Result.Close();
                                                                                           }
                                                                                           catch (Exception)
                                                                                           {
                                                                                               // we expect an exception, because we aborted the connection
                                                                                           }
                                                                                       });
                       observableLineStream.Start();
                       return (IObservable<string>)observableLineStream;
                   })
                   .ContinueWith(task =>
                   {
                       var webException = task.Exception.ExtractSingleInnerException() as WebException;
                       if (webException == null || retries >= 3 || disabledAuthRetries)
                           return task;// effectively throw

                       var httpWebResponse = webException.Response as HttpWebResponse;
                       if (httpWebResponse == null ||
                            (httpWebResponse.StatusCode != HttpStatusCode.Unauthorized &&
                             httpWebResponse.StatusCode != HttpStatusCode.Forbidden &&
                             httpWebResponse.StatusCode != HttpStatusCode.PreconditionFailed))
                           return task; // effectively throw

                       if (httpWebResponse.StatusCode == HttpStatusCode.Forbidden)
                       {
                           HandleForbiddenResponseAsync(httpWebResponse);
                           return task;
                       }

                       var authorizeResponse = HandleUnauthorizedResponseAsync(httpWebResponse);

                       if (authorizeResponse == null)
                           return task; // effectively throw

                       return authorizeResponse
                           .ContinueWith(_ =>
                           {
                               _.Wait(); //throw on error
                               return ServerPullAsync(retries + 1);
                           })
                           .Unwrap();
                   }).Unwrap();
            })
                .Unwrap();
        }

        public async Task<RavenJToken> ExecuteWriteAsync(string data)
        {
            await WriteAsync(data);
            return await ExecuteRequestAsync();
        }

        public async Task<RavenJToken> ExecuteWriteAsync(byte[] data)
        {
            await WriteAsync(data);
            return await ExecuteRequestAsync();
        }

        public double CalculateDuration()
        {
            return 0;
        }

        public HttpJsonRequest AddReplicationStatusHeaders(string thePrimaryUrl, string currentUrl, ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior, Action<NameValueCollection, string, string> handleReplicationStatusChanges)
        {
            if (thePrimaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
                return this;
            if (replicationInformer.GetFailureCount(thePrimaryUrl) <= 0)
                return this; // not because of failover, no need to do this.

            var lastPrimaryCheck = replicationInformer.GetFailureLastCheck(thePrimaryUrl);
            webRequest.Headers[Constants.RavenClientPrimaryServerUrl] = ToRemoteUrl(thePrimaryUrl);
            webRequest.Headers[Constants.RavenClientPrimaryServerLastCheck] = lastPrimaryCheck.ToString("s");

            primaryUrl = thePrimaryUrl;
            operationUrl = currentUrl;

            HandleReplicationStatusChanges = handleReplicationStatusChanges;

            return this;
        }

        private static string ToRemoteUrl(string primaryUrl)
        {
            var uriBuilder = new UriBuilder(primaryUrl);
            return uriBuilder.Uri.ToString();
        }

        public void PrepareForLongRequest()
        {
            Timeout = TimeSpan.FromHours(6);
            webRequest.AllowWriteStreamBuffering = false;
        }

        public Task<Stream> GetRawRequestStream()
        {
            return Task.Factory.FromAsync<Stream>(webRequest.BeginGetRequestStream, webRequest.EndGetRequestStream, null);
        }

        public async Task<WebResponse> RawExecuteRequestAsync()
        {
            if (requestSendToServer)
                throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");

            requestSendToServer = true;
            webRequest.AllowReadStreamBuffering = false;
            webRequest.AllowWriteStreamBuffering = false;
            await WaitForTask;
            try
            {
                return await webRequest.GetResponseAsync();
            }
            catch (SecurityException e)
            {
                throw new WebException(
                    "Could not contact server.\r\nGot security error because RavenDB wasn't able to contact the database to get ClientAccessPolicy.xml permission.",
                    e)
                {
                    Data = { { "Url", webRequest.RequestUri } }
                };
            }
            catch (Exception e)
            {
                e.Data["Url"] = webRequest.RequestUri;
                throw;
            }
        }
    }
}