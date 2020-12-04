//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

#if !DNXCORE50
using System.Runtime.Remoting.Messaging;
#endif

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;
using Raven.Abstractions;
using Raven.Client.Extensions;
using Raven.Client.Metrics;
using Raven.Client.Util;

namespace Raven.Client.Connection.Implementation
{
    /// <summary>
    /// A representation of an HTTP json request to the RavenDB server
    /// </summary>
    public class HttpJsonRequest : IDisposable
    {
        public const int MinimumServerVersion = 3000;
        public const int CustomBuildVersion = 13;

        internal string Url
        {
            get { return url; }
            set { url = value; }
        }
        internal readonly HttpMethod Method;

        internal volatile HttpClient httpClient;

        private readonly NameValueCollection headers = new NameValueCollection();

        private readonly Stopwatch sp = Stopwatch.StartNew();

        private readonly OperationCredentials _credentials;

        // temporary create a strong reference to the cached data for this request
        // avoid the potential for clearing the cache from a cached item
        internal CachedRequest CachedRequestDetails;
        private readonly HttpJsonRequestFactory factory;
        private readonly Func<HttpMessageHandler> recreateHandler;
        private readonly IHoldProfilingInformation owner;
        private readonly ConventionBase conventions;
        private readonly bool disabledAuthRetries;
        private readonly IRequestTimeMetric requestTimeMetric;

        private string postedData;
        private bool isRequestSentToServer;

        internal bool ShouldCacheRequest;
        private Stream postedStream;
        private bool writeCalled;
        public static readonly string ClientVersion = typeof(HttpJsonRequest).Assembly().GetName().Version.ToString();

        private string primaryUrl;

        private string operationUrl;

        public Action<NameValueCollection, string, string> HandleReplicationStatusChanges = delegate { };
        private string url;
        private bool criticalError;

        /// <summary>
        /// Gets or sets the response headers.
        /// </summary>
        /// <value>The response headers.</value>
        public NameValueCollection ResponseHeaders { get; set; }

        internal HttpJsonRequest(
            CreateHttpJsonRequestParams requestParams,
            HttpJsonRequestFactory factory)
        {
            _credentials = requestParams.DisableAuthentication == false ? requestParams.Credentials : null;
            disabledAuthRetries = requestParams.DisableAuthentication;

            Url = requestParams.Url;
            Method = requestParams.Method;

            if (requestParams.Timeout.HasValue)
            {
                Timeout = requestParams.Timeout.Value;
            }
            else
            {
                Timeout = TimeSpan.FromSeconds(100); // default HttpClient timeout
#if DEBUG
                if (Debugger.IsAttached)
                {
                    Timeout = TimeSpan.FromMinutes(5);
                }
#endif
            }

            this.factory = factory;
            owner = requestParams.Owner;
            conventions = requestParams.Convention;
            requestTimeMetric = requestParams.RequestTimeMetric;

            recreateHandler = factory.httpMessageHandler ?? (
                () =>
                {
                    var useDefaultCredentials = _credentials != null && _credentials.HasCredentials() == false;
                    ICredentials credentialsToUse = null;
                    if (_credentials != null)
                    {
                        var networkCredentials = _credentials.Credentials as NetworkCredential;
                        if (networkCredentials != null && factory.authenticationScheme != null)
                        {
                            var credentialCache = new CredentialCache();
                            var uri = new Uri(requestParams.Url);
                            credentialCache.Add(new Uri(string.Format("{0}://{1}:{2}/", uri.Scheme, uri.Host, uri.Port)), factory.authenticationScheme, networkCredentials);

                            credentialsToUse = credentialCache;
                        }
                        else
                        {
                            credentialsToUse = _credentials.Credentials;
                        }
                    }
#if !DNXCORE50
                    var handler = new WebRequestHandler
                    {
                        AllowAutoRedirect = false,
                        UseDefaultCredentials = useDefaultCredentials,
                        Credentials = credentialsToUse
                    };
#else
                    var handler = new HttpClientHandler
                    {
                        AllowAutoRedirect = false,
                        UseDefaultCredentials = useDefaultCredentials,
                        Credentials = credentialsToUse
                    };
#endif
                    return handler;
                }
            );

            httpClient = factory.httpClientCache.GetClient(Timeout, _credentials, recreateHandler);

            if (factory.DisableRequestCompression == false && requestParams.DisableRequestCompression == false)
            {
                if (Method == HttpMethods.Post || Method == HttpMethods.Put || Method == HttpMethods.Patch || Method == HttpMethods.Eval)
                {
                    httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Encoding", "gzip");
                    httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json; charset=utf-8");
                }

                if (factory.acceptGzipContent)
                    httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
            }

            headers.Add("Raven-Client-Version", ClientVersion);
            WriteMetadata(requestParams.Metadata);
            requestParams.UpdateHeaders(headers);
        }

        public void RemoveAuthorizationHeader()
        {
            httpClient.DefaultRequestHeaders.Remove("Authorization");
        }

        public Task ExecuteRequestAsync()
        {
            return ReadResponseJsonAsync();
        }

        /// <summary>
        /// Begins the read response string.
        /// </summary>
        public async Task<RavenJToken> ReadResponseJsonAsync()
        {
            if (SkipServerCheck)
            {
                var cachedResult = factory.GetCachedResponse(this);
                if (factory.CanLogRequest)
                {
                    factory.OnLogRequest(owner, new RequestResultArgs
                    {
                        DurationMilliseconds = CalculateDuration(),
                        Method = Method,
                        HttpResult = (int)ResponseStatusCode,
                        Status = RequestStatus.AggressivelyCached,
                        Result = cachedResult.ToString(),
                        Url = Url,
                        PostedData = postedData
                    });
                }

                return cachedResult;
            }

            if (writeCalled)
                return await ReadJsonInternalAsync().ConfigureAwait(false);

            var result = await SendRequestInternal(() => new HttpRequestMessage(Method, Url)).ConfigureAwait(false);
            if (result != null)
                return result;
            return await ReadJsonInternalAsync().ConfigureAwait(false);
        }

        private Task<RavenJToken> SendRequestInternal(Func<HttpRequestMessage> getRequestMessage, bool readErrorString = true)
        {
            if (isRequestSentToServer && Debugger.IsAttached == false)
                throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");
            isRequestSentToServer = true;

            return RunWithAuthRetry(async () =>
            {
                try
                {
                    var requestMessage = getRequestMessage();
                    CopyHeadersToHttpRequestMessage(requestMessage);
                    Response = await httpClient.SendAsync(requestMessage).ConfigureAwait(false);
                    SetResponseHeaders(Response);
                    AssertServerVersionSupported();
                    ResponseStatusCode = Response.StatusCode;
                }
                catch (HttpRequestException e)
                {
                    var exception = ErrorResponseException.FromException(e);
                    exception.Data.Add(Constants.RequestFailedExceptionMarker, true);
                    Response = exception.Response;
                    ResponseStatusCode = Response.StatusCode;
                    throw exception;
                }
                catch (TaskCanceledException e)
                {
                    var exception = ErrorResponseException.FromException(e);
                    exception.Data.Add(Constants.RequestFailedExceptionMarker, true);
                    Response = exception.Response;
                    ResponseStatusCode = Response.StatusCode;
                    throw exception;
                }
                finally
                {
                    sp.Stop();
                    if (requestTimeMetric != null)
                        requestTimeMetric.Update((int)sp.Elapsed.TotalMilliseconds);
                }

                // throw the conflict exception
                return await CheckForErrorsAndReturnCachedResultIfAnyAsync(readErrorString).ConfigureAwait(false);
            });
        }

        private void AssertServerVersionSupported()
        {
#if !DNXCORE50
            if ((CallContext.GetData(Constants.Smuggler.CallContext) as bool?) == true) // allow Raven.Smuggler to work against old servers
                return;
#endif

            var serverBuildString = ResponseHeaders[Constants.RavenServerBuild];
            int serverBuild;

            // server doesn't return Raven-Server-Build in case of requests failures, thus we firstly check for header presence 
            if (string.IsNullOrEmpty(serverBuildString) == false && int.TryParse(serverBuildString, out serverBuild))
            {
                if (serverBuild < MinimumServerVersion && serverBuild != CustomBuildVersion)
                {
                    throw new ServerVersionNotSuppportedException(string.Format("Server version {0} is not supported. Use server with build >= {1}", serverBuildString, MinimumServerVersion));
                }
            }

        }

        private async Task<T> RunWithAuthRetry<T>(Func<Task<T>> requestOperation)
        {
            int retries = 0;
            while (true)
            {
                ErrorResponseException responseException;
                try
                {
                    var opResult = await requestOperation().ConfigureAwait(false);
                    return opResult;
                }
                catch (ErrorResponseException e)
                {
                    if (++retries >= 3 || disabledAuthRetries)
                        throw;

                    if (e.StatusCode != HttpStatusCode.Unauthorized &&
                        e.StatusCode != HttpStatusCode.Forbidden &&
                        e.StatusCode != HttpStatusCode.PreconditionFailed)
                        throw;

                    responseException = e;
                }
                catch (IndexCompilationException)
                {
                    throw;
                }
                catch (BadRequestException)
                {
                    throw;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception)
                {
                    criticalError = true;
                    throw;
                }

                if (ResponseStatusCode == HttpStatusCode.Forbidden)
                {
                    await HandleForbiddenResponseAsync(Response).ConfigureAwait(false);
                    throw responseException;
                }

                if (await HandleUnauthorizedResponseAsync(Response).ConfigureAwait(false) == false)
                    throw responseException;
            }
        }

        private void CopyHeadersToHttpRequestMessage(HttpRequestMessage httpRequestMessage)
        {
            for (int i = 0; i < headers.Count; i++)
            {
                var key = headers.GetKey(i);
                var values = headers.GetValues(i);
                Debug.Assert(values != null);
                httpRequestMessage.Headers.TryAddWithoutValidation(key, values);
            }
        }

        private void SetResponseHeaders(HttpResponseMessage response)
        {
            ResponseHeaders = new NameValueCollection();
            foreach (var header in response.Headers)
            {
                foreach (var val in header.Value)
                {
                    ResponseHeaders.Add(header.Key, val);
                }
            }
            foreach (var header in response.Content.Headers)
            {
                foreach (var val in header.Value)
                {
                    ResponseHeaders.Add(header.Key, val);
                }
            }
        }

        private async Task<RavenJToken> CheckForErrorsAndReturnCachedResultIfAnyAsync(bool readErrorString)
        {
            if (Response.IsSuccessStatusCode)
                return null;
            if (ResponseStatusCode == HttpStatusCode.Unauthorized ||
                ResponseStatusCode == HttpStatusCode.NotFound ||
                ResponseStatusCode == HttpStatusCode.Conflict)
            {
                if (factory.CanLogRequest)
                {
                    factory.OnLogRequest(owner, new RequestResultArgs
                    {
                        DurationMilliseconds = CalculateDuration(),
                        Method = Method,
                        HttpResult = (int)ResponseStatusCode,
                        Status = RequestStatus.ErrorOnServer,
                        Result = ResponseStatusCode.ToString(),
                        Url = Url,
                        PostedData = postedData
                    });
                }


                throw ErrorResponseException.FromResponseMessage(Response, readErrorString);
            }

            if (ResponseStatusCode == HttpStatusCode.NotModified
                && CachedRequestDetails != null)
            {
                factory.UpdateCacheTime(this);
                var result = factory.GetCachedResponse(this, ResponseHeaders);

                // here we explicitly need to get Response.Headers, and NOT ResponseHeaders because we are 
                // getting the value _right now_ from the secondary, and don't care about the 304, the force check
                // is still valid
                HandleReplicationStatusChanges(ResponseHeaders, primaryUrl, operationUrl);

                if (factory.CanLogRequest)
                {
                    factory.OnLogRequest(owner, new RequestResultArgs
                    {
                        DurationMilliseconds = CalculateDuration(),
                        Method = Method,
                        HttpResult = (int)ResponseStatusCode,
                        Status = RequestStatus.Cached,
                        Result = result.ToString(),
                        Url = Url,
                        PostedData = postedData
                    });
                }


                return result;
            }


            using (var sr = new StreamReader(await Response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false)))
            {
                var readToEnd = sr.ReadToEnd();

                if (factory.CanLogRequest)
                {
                    factory.OnLogRequest(owner, new RequestResultArgs
                    {
                        DurationMilliseconds = CalculateDuration(),
                        Method = Method,
                        HttpResult = (int)ResponseStatusCode,
                        Status = RequestStatus.Cached,
                        Result = readToEnd,
                        Url = Url,
                        PostedData = postedData
                    });
                }

                if (string.IsNullOrWhiteSpace(readToEnd))
                    throw ErrorResponseException.FromResponseMessage(Response);

                RavenJObject ravenJObject;
                try
                {
                    ravenJObject = RavenJObject.Parse(readToEnd);
                }
                catch (Exception e)
                {
                    throw new ErrorResponseException(Response, readToEnd, e);
                }
                if (ravenJObject.ContainsKey("IndexDefinitionProperty"))
                {
                    throw new IndexCompilationException(ravenJObject.Value<string>("Message"))
                    {
                        IndexDefinitionProperty = ravenJObject.Value<string>("IndexDefinitionProperty"),
                        ProblematicText = ravenJObject.Value<string>("ProblematicText")
                    };
                }
                //else if (ravenJObject.ContainsKey())
                if (ResponseStatusCode == HttpStatusCode.BadRequest && ravenJObject.ContainsKey("Message"))
                {
                    throw new BadRequestException(ravenJObject.Value<string>("Message"), ErrorResponseException.FromResponseMessage(Response));
                }
                if (ravenJObject.ContainsKey("Error"))
                {
                    var errorValue = ravenJObject.Value<string>("Error");
                    if (errorValue != null && errorValue.StartsWith("System.TimeoutException"))
                    {
                        throw new TimeoutException(errorValue.Substring("System.TimeoutException: ".Length));
                    }

                    var sb = new StringBuilder();
                    foreach (var prop in ravenJObject)
                    {
                        if (prop.Key == "Error")
                            continue;

                        sb.Append(prop.Key).Append(": ").AppendLine(prop.Value.ToString(Formatting.Indented));
                    }

                    if (sb.Length > 0)
                        sb.AppendLine();
                    sb.Append(errorValue);

                    throw new ErrorResponseException(Response, sb.ToString(), readToEnd);
                }
                throw new ErrorResponseException(Response, readToEnd);
            }
        }

        public async Task<byte[]> ReadResponseBytesAsync()
        {
            await SendRequestInternal(() => new HttpRequestMessage(Method, Url), readErrorString: false).ConfigureAwait(false);

            using (var stream = await Response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
            {
                return await stream.ReadDataAsync().ConfigureAwait(false);
            }
        }

        public void ExecuteRequest()
        {
            ReadResponseJson();
        }

        public RavenJToken ReadResponseJson()
        {
            return AsyncHelpers.RunSync(ReadResponseJsonAsync);
        }

        public async Task<bool> HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse)
        {
            if (conventions == null || conventions.HandleUnauthorizedResponseAsync == null)
                return false;

            var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse, _credentials);
            if (unauthorizedResponseAsync == null)
                return false;

            var configureHttpClient = await unauthorizedResponseAsync.ConfigureAwait(false);
            RecreateHttpClient(configureHttpClient);
            return true;
        }

        private async Task HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse)
        {
            if (conventions == null || conventions.HandleForbiddenResponseAsync == null)
                return;

            var forbiddenResponseAsync = conventions.HandleForbiddenResponseAsync(forbiddenResponse, _credentials);
            if (forbiddenResponseAsync == null)
                return;

            await forbiddenResponseAsync.ConfigureAwait(false);
        }

        private void RecreateHttpClient(Action<HttpClient> configureHttpClient)
        {
            var newHttpClient = factory.httpClientCache.GetClient(Timeout, _credentials, recreateHandler);
            configureHttpClient(newHttpClient);

            DisposeInternal();

            httpClient = newHttpClient;
            isRequestSentToServer = false;

            if (postedStream != null)
            {
                postedStream.Position = 0;
            }
        }

        public long Size { get; private set; }

        private async Task<RavenJToken> ReadJsonInternalAsync()
        {
            await Response.AssertNotFailingResponse().ConfigureAwait(false);

            HandleReplicationStatusChanges(ResponseHeaders, primaryUrl, operationUrl);

            using (var responseStream = await Response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
            {
                var countingStream = new CountingStream(responseStream);
                var data = await RavenJToken.TryLoadAsync(countingStream).ConfigureAwait(false);
                Size = countingStream.NumberOfReadBytes;

                if (Method == HttpMethods.Get && ShouldCacheRequest)
                {
                    factory.CacheResponse(Url, data, ResponseHeaders);
                }

                if (factory.CanLogRequest)
                {
                    factory.OnLogRequest(owner, new RequestResultArgs
                    {
                        DurationMilliseconds = CalculateDuration(),
                        Method = Method,
                        HttpResult = (int)ResponseStatusCode,
                        Status = RequestStatus.SentToServer,
                        Result = (data ?? "").ToString(),
                        Url = Url,
                        PostedData = postedData
                    });
                }

                return data;
            }
        }

        /// <summary>
        /// Adds the operation headers.
        /// </summary>
        /// <param name="operationsHeaders">The operations headers.</param>
        public HttpJsonRequest AddOperationHeaders(NameValueCollection operationsHeaders)
        {
            headers.Add(operationsHeaders);
            return this;
        }

        /// <summary>
        /// Adds the operation header.
        /// </summary>
        public HttpJsonRequest AddOperationHeader(string key, string value)
        {
            headers[key] = value;
            return this;
        }

        public HttpJsonRequest AddRequestExecuterAndReplicationHeaders(
            AsyncServerClient serverClient,
            string currentUrl, bool withClusterFailoverHeader = false)
        {
            serverClient.RequestExecuter.AddHeaders(this, serverClient, currentUrl,withClusterFailoverHeader);
            return this; // not because of failover, no need to do this.
        }

        internal void AddReplicationStatusChangeBehavior(string thePrimaryUrl, string currentUrl, Action<NameValueCollection, string, string> handler)
        {
            HandleReplicationStatusChanges = handler;
            primaryUrl = thePrimaryUrl;
            operationUrl = currentUrl;
        }

        /// <summary>
        /// The request duration
        /// </summary>
        public double CalculateDuration()
        {
            return sp.ElapsedMilliseconds;
        }

        /// <summary>
        /// Gets or sets the response status code.
        /// </summary>
        /// <value>The response status code.</value>
        public HttpStatusCode ResponseStatusCode { get; set; }

        ///<summary>
        /// Whatever we can skip the server check and directly return the cached result
        ///</summary>
        public bool SkipServerCheck { get; set; }

        public TimeSpan Timeout { get; private set; }

        public HttpResponseMessage Response { get; private set; }

        private void WriteMetadata(RavenJObject metadata)
        {
            if (metadata == null || metadata.Count == 0)
                return;

            foreach (var prop in metadata)
            {
                if (prop.Value == null || prop.Value.Type == JTokenType.Null)
                    continue;

                if (prop.Value.Type == JTokenType.Object ||
                    prop.Value.Type == JTokenType.Array)
                    continue;

                var headerName = prop.Key;
                var value = prop.Value.Value<object>().ToString();
                if (headerName == Constants.MetadataEtagField)
                {
                    headerName = "If-None-Match";
                    if (!value.StartsWith("\""))
                    {
                        value = "\"" + value;
                    }
                    if (!value.EndsWith("\""))
                    {
                        value = value + "\"";
                    }
                }

                bool isRestricted;
                try
                {
#if !DNXCORE50
                    isRestricted = WebHeaderCollection.IsRestricted(headerName);
#else
                    // TODO [ppekrol] Check if this is OK
                    isRestricted = false;
#endif
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not figure out how to treat header: " + headerName, e);
                }
                // Restricted headers require their own special treatment, otherwise an exception will
                // be thrown.
                // See http://msdn.microsoft.com/en-us/library/78h415ay.aspx
                if (isRestricted)
                {
                    switch (headerName)
                    {
                        /*case "Date":
                        case "Referer":
                        case "Content-Length":
                        case "Expect":
                        case "Range":
                        case "Transfer-Encoding":
                        case "User-Agent":
                        case "Proxy-Connection":
                        case "Host": // Host property is not supported by 3.5
                            break;*/
                        case "Content-Type":
                            headers["Content-Type"] = value;
                            break;
                        case "If-Modified-Since":
                            DateTime tmp;
                            DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out tmp);
                            httpClient.DefaultRequestHeaders.IfModifiedSince = tmp;
                            break;
                        case "Accept":
                            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(value));
                            break;
                        case "Connection":
                            httpClient.DefaultRequestHeaders.Connection.Add(value);
                            break;
                    }
                }
                else
                {
                    headers[headerName] = value;
                }
            }
        }

        public async Task<IObservable<string>> ServerPullAsync()
        {
            return await RunWithAuthRetry(async () =>
            {
                var httpRequestMessage = new HttpRequestMessage(Method, Url);
                Response = await httpClient.SendAsync(httpRequestMessage, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
                SetResponseHeaders(Response);
                AssertServerVersionSupported();

                await CheckForErrorsAndReturnCachedResultIfAnyAsync(readErrorString: true).ConfigureAwait(false);

                var stream = await Response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                var observableLineStream = new ObservableLineStream(stream, () =>
                {
                    Response.Dispose();
                    factory.HttpClientCache.ReleaseClient(httpClient, _credentials);
                });
                observableLineStream.Start();
                return (IObservable<string>)observableLineStream;
            }).ConfigureAwait(false);
        }

        public Task WriteWithObjectAsync<T>(IEnumerable<T> data)
        {
            return WriteAsync(JsonExtensions.ToJArray(data));
        }

        public Task WriteWithObjectAsync<T>(T data)
        {
            if (data is IEnumerable)
                throw new ArgumentException("The object implements IEnumerable. This method cannot handle it. Give the type system some hint with the 'as IEnumerable' statement to help the compiler to select the correct overload.");

            return WriteAsync(JsonExtensions.ToJObject(data));
        }

        public Task WriteAsync(RavenJToken tokenToWrite)
        {
            writeCalled = true;

            return SendRequestInternal(() =>
            {
                HttpContent content = new JsonContent(tokenToWrite);
                content = new CompressedContent(content, "gzip");

                return new HttpRequestMessage(Method, Url)
                {
                    Content = content,
                    Headers =
                    {
                        TransferEncodingChunked = !EnvironmentUtils.RunningOnPosix
                    }
                };
            });
        }

        public Task WriteAsync(Stream streamToWrite)
        {
            postedStream = streamToWrite;
            writeCalled = true;

            return SendRequestInternal(() => new HttpRequestMessage(Method, Url)
            {
                Content = new CompressedStreamContent(streamToWrite, factory.DisableRequestCompression, disposeStream: false).SetContentType(headers)
            });
        }

        public Task WriteAsync(HttpContent content)
        {
            writeCalled = true;

            return SendRequestInternal(() => new HttpRequestMessage(Method, Url)
            {
                Content = content,
                Headers =
                {
                        TransferEncodingChunked = !EnvironmentUtils.RunningOnPosix,
                }
            });
        }

        public Task WriteAsync(string data)
        {
            postedData = data;
            writeCalled = true;

            return SendRequestInternal(() =>
            {
                var request = new HttpRequestMessage(Method, Url)
                {
                    Content = new CompressedStringContent(data, factory.DisableRequestCompression),
                };
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
                return request;
            });
        }

        public Task<HttpResponseMessage> ExecuteRawResponseAsync(RavenJToken token)
        {
            return ExecuteRawResponseInternalAsync(new JsonContent(token));
        }

        public Task<HttpResponseMessage> ExecuteRawResponseAsync(string data, string contentType = null)
        {
            return ExecuteRawResponseInternalAsync(new CompressedStringContent(data, factory.DisableRequestCompression, contentType));
        }

        public Task<HttpResponseMessage> ExecuteRawResponseAsync()
        {
            return ExecuteRawResponseInternalAsync(null);
        }

        private async Task<HttpResponseMessage> ExecuteRawResponseInternalAsync(HttpContent content)
        {
            return await RunWithAuthRetry(async () =>
            {
                var rawRequestMessage = new HttpRequestMessage(Method, Url);

                if (content != null)
                {
                    rawRequestMessage.Content = content;
                }

                CopyHeadersToHttpRequestMessage(rawRequestMessage);

                Response = await httpClient.SendAsync(rawRequestMessage, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);

                ResponseStatusCode = Response.StatusCode;
                if (Response.IsSuccessStatusCode == false &&
                    (ResponseStatusCode == HttpStatusCode.PreconditionFailed ||
                    ResponseStatusCode == HttpStatusCode.Forbidden ||
                    ResponseStatusCode == HttpStatusCode.Unauthorized))
                {
                    throw new ErrorResponseException(Response, "Failed request");
                }

                return Response;
            }).ConfigureAwait(false);
        }

        public async Task<HttpResponseMessage> ExecuteRawRequestAsync(Action<Stream, TaskCompletionSource<object>> action)
        {
            httpClient.DefaultRequestHeaders.TransferEncodingChunked = !EnvironmentUtils.RunningOnPosix;

            return await RunWithAuthRetry(async () =>
            {
                var rawRequestMessage = new HttpRequestMessage(Method, Url)
                {
                    Content = new PushContent(action)
                };

                CopyHeadersToHttpRequestMessage(rawRequestMessage);
                Response = await httpClient.SendAsync(rawRequestMessage).ConfigureAwait(false);
                ResponseStatusCode = Response.StatusCode;

                if (Response.IsSuccessStatusCode == false &&
                    (ResponseStatusCode == HttpStatusCode.PreconditionFailed ||
                    ResponseStatusCode == HttpStatusCode.Forbidden ||
                    ResponseStatusCode == HttpStatusCode.Unauthorized))
                {
                    throw new ErrorResponseException(Response, "Failed request");
                }

                return Response;
            }).ConfigureAwait(false);
        }

        private class PushContent : HttpContent
        {
            private readonly Action<Stream, TaskCompletionSource<object>> action;

            private readonly TaskCompletionSource<object> tcs = TaskCompletionSourceFactory.Create<object>();

            public PushContent(Action<Stream, TaskCompletionSource<object>> action)
            {
                this.action = action;
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                action(stream, tcs);
                return tcs.Task;
            }

            protected override bool TryComputeLength(out long length)
            {
                length = -1;
                return false;
            }
        }

        public void AddHeader(string key, string val)
        {
            headers.Set(key, val);
        }

        public void AddRange(long @from, long? to = null)
        {
            httpClient.DefaultRequestHeaders.Range = new RangeHeaderValue(from, to);
        }

        public void AddHeaders(RavenJObject headersToAdd)
        {
            foreach (var item in headersToAdd)
            {
                switch (item.Value.Type)
                {
                    case JTokenType.Object:
                    case JTokenType.Array:
                        AddHeader(item.Key, item.Value.ToString(Formatting.None));
                        break;
                    case JTokenType.Date:
                        var rfc1123 = GetDateString(item.Value, "r");
                        var iso8601 = GetDateString(item.Value, "o");
                        AddHeader(item.Key, rfc1123);
                        if (item.Key.StartsWith("Raven-") == false)
                            AddHeader("Raven-" + item.Key, iso8601);
                        break;
                    default:
                        AddHeader(item.Key, item.Value.Value<string>());
                        break;
                }
            }
        }

        private string GetDateString(RavenJToken token, string format)
        {
            var value = token as RavenJValue;
            if (value == null)
                return token.ToString();

            var obj = value.Value;

            if (obj is DateTime)
                return ((DateTime)obj).ToString(format);

            if (obj is DateTimeOffset)
                return ((DateTimeOffset)obj).ToString(format);

            return obj.ToString();
        }

        public void AddHeaders(NameValueCollection nameValueHeaders)
        {
            foreach (var key in nameValueHeaders.AllKeys)
            {
                AddHeader(key, nameValueHeaders[key]);
            }
        }

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

            if (httpClient != null)
            {
                if (criticalError == false)
                    factory.httpClientCache.ReleaseClient(httpClient, _credentials);
                else
                    httpClient.Dispose();

                httpClient = null;
            }
        }

        public IDisposable Continue100Scope()
        {
            return ConnectionOptions.Expect100Continue(this.Url);
        }
    }
}
