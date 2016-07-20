#if !DNXCORE50
using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Connection
{
    using System.Threading;

    public class HttpRavenRequest
    {
        private readonly string url;
        private readonly HttpMethod httpMethod;
        private readonly Action<RavenConnectionStringOptions, HttpWebRequest> configureRequest;
        private readonly Func<RavenConnectionStringOptions, WebResponse, Action<HttpWebRequest>> handleUnauthorizedResponse;
        private readonly RavenConnectionStringOptions connectionStringOptions;

        private readonly bool? allowWriteStreamBuffering;

        private HttpWebRequest webRequest;

        private Stream postedStream;
        private RavenJToken postedToken;
        private byte[] postedData;
        private bool writeBson;

        public long NumberOfBytesWrittenCompressed { get; private set; }
        public long NumberOfBytesWrittenUncompressed { get; private set; }

        public HttpWebRequest WebRequest
        {
            get { return webRequest ?? (webRequest = CreateRequest()); }
            set { webRequest = value; }
        }

        public HttpRavenRequest(string url, HttpMethod httpMethod, Action<RavenConnectionStringOptions, HttpWebRequest> configureRequest, Func<RavenConnectionStringOptions, WebResponse, Action<HttpWebRequest>> handleUnauthorizedResponse, RavenConnectionStringOptions connectionStringOptions, bool? allowWriteStreamBuffering)
        {
            this.url = url;
            this.httpMethod = httpMethod;
            this.configureRequest = configureRequest;
            this.handleUnauthorizedResponse = handleUnauthorizedResponse;
            this.connectionStringOptions = connectionStringOptions;
            this.allowWriteStreamBuffering = allowWriteStreamBuffering;
        }

        private HttpWebRequest CreateRequest()
        {
            var request = (HttpWebRequest)System.Net.WebRequest.Create(url);
            request.Method = httpMethod.Method;
            if (httpMethod == HttpMethods.Post || httpMethod == HttpMethods.Put)
                request.Headers["Content-Encoding"] = "gzip";
            request.Headers["Accept-Encoding"] = "deflate,gzip";
            request.ContentType = "application/json; charset=utf-8";

            if (allowWriteStreamBuffering.HasValue)
            {
                request.AllowWriteStreamBuffering = allowWriteStreamBuffering.Value;
                if (allowWriteStreamBuffering.Value == false)
                    request.SendChunked = !EnvironmentUtils.RunningOnPosix;
            }

            configureRequest(connectionStringOptions, request);

            return request;
        }

        public void Write(Stream streamToWrite)
        {
            postedStream = streamToWrite;
            if (EnvironmentUtils.RunningOnPosix) // mono must set ContentLength before GetRequestStream (unlike .net)
                WebRequest.ContentLength = streamToWrite.Length;
            
            using (var stream = WebRequest.GetRequestStream())
            using (var countingStream = new CountingStream(stream, l => NumberOfBytesWrittenCompressed = l))
            using (var commpressedStream = new GZipStream(countingStream, CompressionMode.Compress))
            using (var countingStream2 = new CountingStream(commpressedStream, l => NumberOfBytesWrittenUncompressed = l))
            {
                streamToWrite.CopyTo(countingStream2);
                commpressedStream.Flush();
                stream.Flush();
            }
        }

        public void Write(RavenJToken ravenJToken)
        {
            postedToken = ravenJToken;
            WriteToken(WebRequest);
        }

        public void Write(byte[] data)
        {
            postedData = data;
            if (EnvironmentUtils.RunningOnPosix) // mono must set ContentLength before GetRequestStream (unlike .net)
                WebRequest.ContentLength = data.Length; 
            
            using (var stream = WebRequest.GetRequestStream())
            using (var countingStream = new CountingStream(stream, l => NumberOfBytesWrittenCompressed = l))
            using (var cmp = new GZipStream(countingStream, CompressionMode.Compress))
            using (var countingStream2 = new CountingStream(cmp, l => NumberOfBytesWrittenUncompressed = l))
            {
                countingStream2.Write(data, 0, data.Length);
                cmp.Flush();
                stream.Flush();
            }
        }

        public void WriteBson(RavenJToken ravenJToken)
        {
            writeBson = true;
            postedToken = ravenJToken;
            WriteToken(WebRequest);
        }

        private void WriteToken(WebRequest httpWebRequest)
        {
            if (EnvironmentUtils.RunningOnPosix) // mono must set ContentLength before GetRequestStream (unlike .net)
                httpWebRequest.ContentLength = postedToken.ToString().Length;

            using (var stream = httpWebRequest.GetRequestStream())
            {
                using (var countingStream = new CountingStream(stream, l => NumberOfBytesWrittenCompressed = l))
                using (var commpressedData = new GZipStream(countingStream, CompressionMode.Compress))
                using (var countingStream2 = new CountingStream(commpressedData, l => NumberOfBytesWrittenUncompressed = l))
                {
                    if (writeBson)
                    {
                        postedToken.WriteTo(new BsonWriter(countingStream2));
                    }
                    else
                    {
                        var streamWriter = new StreamWriter(countingStream2);
                        postedToken.WriteTo(new JsonTextWriter(streamWriter));
                        streamWriter.Flush();
                    }
                    commpressedData.Flush();
                    stream.Flush();
                }
            }
        }

        public T ExecuteRequest<T>()
        {
            T result = default(T);
            SendRequestToServer(response =>
                                    {
                                        using (var stream = response.GetResponseStreamWithHttpDecompression())
                                        using (var reader = new StreamReader(stream))
                                        {
                                            result = reader.JsonDeserialization<T>();
                                        }
                                    });
            return result;
        }

        public void ExecuteRequest(Action<TextReader> action)
        {
            SendRequestToServer(response =>
            {
                using (var stream = response.GetResponseStreamWithHttpDecompression())
                using (var reader = new StreamReader(stream))
                {
                    action(reader);
                }
            });
        }

        public void ExecuteRequest(Action<Stream> action)
        {
            SendRequestToServer(response =>
            {
                using (var stream = response.GetResponseStreamWithHttpDecompression())
                {
                    action(stream);
                }
            });
        }

        public void ExecuteRequest()
        {
            SendRequestToServer(response => { });
        }

        public void ExecuteRequest(CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => WebRequest.Abort()))
            {
                try
                {
                    SendRequestToServer(response => { });
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException(ex.Message, ex, cancellationToken);
                    
                    throw;
                }
            }	
        }

        private void SendRequestToServer(Action<WebResponse> action)
        {
            int retries = 0;
            while (true)
            {
                try
                {
                    if (WebRequest.Method != "GET" && postedData == null && postedStream == null && postedToken == null)
                        WebRequest.ContentLength = 0;
                        
                    using (var res = WebRequest.GetResponse())
                    {
                        action(res);
                    }
                    return;
                }
                catch (WebException e)
                {
                    if (++retries >= 3)
                        throw;

                    var response = e.Response as HttpWebResponse;
                    if (response == null)
                        throw;

                    if (response.StatusCode != HttpStatusCode.Unauthorized &&
                        response.StatusCode != HttpStatusCode.PreconditionFailed)
                    {
                        using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
                        {
                            var error = streamReader.ReadToEnd();
                            RavenJObject ravenJObject = null;
                            try
                            {
                                ravenJObject = RavenJObject.Parse(error);
                            }
                            catch { }
                            e.Data["original-value"] = error;
                            if (ravenJObject == null)
                                throw;
                            throw new WebException("Error: " + ravenJObject.Value<string>("Error"), e)
                            {
                                Data = {{"original-value", error}}
                            };
                        }
                    }

                    if (HandleUnauthorizedResponse(e.Response) == false)
                        throw;
                }
            }
        }

        private bool HandleUnauthorizedResponse(WebResponse unauthorizedResponse)
        {
            if (handleUnauthorizedResponse == null)
                return false;

            var unauthorizedResponseAction = handleUnauthorizedResponse(connectionStringOptions, unauthorizedResponse);
            if (unauthorizedResponseAction == null)
                return false;

            RecreateWebRequest(unauthorizedResponseAction);
            return true;
        }

        private void RecreateWebRequest(Action<HttpWebRequest> action)
        {
            // we now need to clone the request, since just calling GetRequest again wouldn't do anything
            var newWebRequest = CreateRequest();
            HttpRequestHelper.CopyHeaders(WebRequest, newWebRequest);
            action(newWebRequest);

            if (postedToken != null)
            {
                WriteToken(newWebRequest);
            }
            if (postedData != null)
            {
                Write(postedData);
            }
            if (postedStream != null)
            {
                postedStream.Position = 0;
                if (EnvironmentUtils.RunningOnPosix) // mono must set ContentLength before GetRequestStream (unlike .net)
                    newWebRequest.ContentLength = postedStream.Length;
                
                using (var stream = newWebRequest.GetRequestStream())
                using (var compressedData = new GZipStream(stream, CompressionMode.Compress))
                {
                    postedStream.CopyTo(compressedData);
                    stream.Flush();
                    compressedData.Flush();
                }
            }
            WebRequest = newWebRequest;
        }

    }
}
#endif