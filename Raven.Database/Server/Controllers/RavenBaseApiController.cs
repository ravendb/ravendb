using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Controllers;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Database.Config;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public abstract class RavenBaseApiController : ApiController
    {
        protected static readonly ILog Log = LogManager.GetCurrentClassLogger();
        
        private HttpRequestMessage request;

        internal bool SkipAuthorizationSinceThisIsMultiGetRequestAlreadyAuthorized{ get; set; }

        public HttpRequestMessage InnerRequest
        {
            get
            {
                return Request ?? request;
            }
        }

        public bool IsInternalRequest
        {
            get
            {
                var internalHeader = GetHeader("Raven-internal-request");
                return internalHeader != null && internalHeader == "true";
            }
        }

        public HttpHeaders InnerHeaders
        {
            get
            {
                HttpRequestMessage message = InnerRequest;
                return CloneRequestHttpHeaders(message.Headers, message.Content == null ? null : message.Content.Headers);
            }
        }

        public static HttpHeaders CloneRequestHttpHeaders( HttpRequestHeaders httpRequestHeaders, HttpContentHeaders httpContentHeaders)
        {
            var headers = new Headers();
            foreach (var header in httpRequestHeaders)
            {
                 headers.Add(header.Key, header.Value);
            }

            if (httpContentHeaders == null)
                return headers;

            foreach (var header in httpContentHeaders)
            {
                headers.Add(header.Key, header.Value);
            }

            return headers; 
        }

        public IEnumerable<KeyValuePair<string,IEnumerable<string>>> ReadInnerHeaders
        {
            get
            {
                foreach (var header in InnerRequest.Headers)
                {
                    yield return new KeyValuePair<string, IEnumerable<string>>(header.Key, header.Value);
                }

                if (InnerRequest.Content == null)
                    yield break;                
                foreach (var header in InnerRequest.Content.Headers)
                {
                    yield return new KeyValuePair<string, IEnumerable<string>>(header.Key, header.Value);
                }
            }
        }

        public new IPrincipal User { get; set; }

        public bool WasAlreadyAuthorizedUsingSingleAuthToken { get; set; }

        protected virtual void InnerInitialization(HttpControllerContext controllerContext)
        {
            request = controllerContext.Request;
            User = controllerContext.RequestContext.Principal;

            landlord = (DatabasesLandlord)controllerContext.Configuration.Properties[typeof(DatabasesLandlord)];
            fileSystemsLandlord = (FileSystemsLandlord)controllerContext.Configuration.Properties[typeof(FileSystemsLandlord)];
            countersLandlord = (CountersLandlord)controllerContext.Configuration.Properties[typeof(CountersLandlord)];
            requestManager = (RequestManager)controllerContext.Configuration.Properties[typeof(RequestManager)];
            maxNumberOfThreadsForDatabaseToLoad = (SemaphoreSlim)controllerContext.Configuration.Properties[Constants.MaxConcurrentRequestsForDatabaseDuringLoad];
            maxSecondsForTaskToWaitForDatabaseToLoad = (int)controllerContext.Configuration.Properties[Constants.MaxSecondsForTaskToWaitForDatabaseToLoad];
            //MaxSecondsForTaskToWaitForDatabaseToLoad
        }

        public async Task<T> ReadJsonObjectAsync<T>()
        {
            using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
            using (var buffered = new BufferedStream(stream))
            using (var gzipStream = new GZipStream(buffered, CompressionMode.Decompress))
            using (var streamReader = new StreamReader(stream, GetRequestEncoding()))
            {
                using (var jsonReader = new JsonTextReader(streamReader))
                {
                    var result = JsonExtensions.CreateDefaultJsonSerializer();

                    return (T)result.Deserialize(jsonReader, typeof(T));
                }
            }
        }

        public async Task<RavenJObject> ReadJsonAsync()
        {
            using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
            using (var buffered = new BufferedStream(stream))
            using (var streamReader = new StreamReader(buffered, GetRequestEncoding()))
            using (var jsonReader = new RavenJsonTextReader(streamReader))
                return RavenJObject.Load(jsonReader);
        }

        public async Task<RavenJArray> ReadJsonArrayAsync()
        {
            using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
            using (var buffered = new BufferedStream(stream))
            using (var streamReader = new StreamReader(buffered, GetRequestEncoding()))
            using (var jsonReader = new RavenJsonTextReader(streamReader))
                return RavenJArray.Load(jsonReader);
        }

        public async Task<string> ReadStringAsync()
        {
            using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
            using (var buffered = new BufferedStream(stream))
            using (var streamReader = new StreamReader(buffered, GetRequestEncoding()))
                return streamReader.ReadToEnd();
        }

        public async Task<RavenJArray> ReadBsonArrayAsync()
        {
            using (var stream = await InnerRequest.Content.ReadAsStreamAsync())
            using (var buffered = new BufferedStream(stream))
            using (var jsonReader = new BsonReader(buffered))
            {
                var jObject = RavenJObject.Load(jsonReader);
                return new RavenJArray(jObject.Values<RavenJToken>());
            }
        }

        private Encoding GetRequestEncoding()
        {
            if (InnerRequest.Content.Headers.ContentType == null || string.IsNullOrWhiteSpace(InnerRequest.Content.Headers.ContentType.CharSet))
                return Encoding.GetEncoding(Constants.DefaultRequestEncoding);
            return Encoding.GetEncoding(InnerRequest.Content.Headers.ContentType.CharSet);
        }

        public int GetStart()
        {
            int start;
            int.TryParse(GetQueryStringValue("start"), out start);
            return Math.Max(0, start);
        }

        public int GetNextPageStart()
        {
            bool isNextPage;
            if (bool.TryParse(GetQueryStringValue("next-page"), out isNextPage) && isNextPage)
                return GetStart();

            return 0;
        }

        public int GetPageSize(int maxPageSize)
        {
            int pageSize;
            if (int.TryParse(GetQueryStringValue("pageSize"), out pageSize) == false)
                pageSize = 25;
            if (pageSize < 0)
                return 0;
            if (pageSize > maxPageSize)
                pageSize = maxPageSize;
            return pageSize;
        }



        public bool MatchEtag(Etag etag)
        {
            return EtagHeaderToEtag() == etag;
        }

        internal Etag EtagHeaderToEtag()
        {
            try
            {
                var responseHeader = GetHeader("If-None-Match");
                if (string.IsNullOrEmpty(responseHeader))
                    return Etag.InvalidEtag;

                if (responseHeader[0] == '\"')
                    return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

                return Etag.Parse(responseHeader);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return Etag.InvalidEtag;
            }
        }

        public string GetQueryStringValue(string key)
        {
            return GetQueryStringValue(InnerRequest, key);
        }

//		public static string GetQueryStringValue(HttpRequestMessage req, string key)
//		{
//			var value = req.GetQueryNameValuePairs().Where(pair => pair.Key == key).Select(pair => pair.Value).FirstOrDefault();
//			if (value != null)
//				value = Uri.UnescapeDataString(value);
//			return value;
//		}


        public static string GetQueryStringValue(HttpRequestMessage req, string key)
        {
            NameValueCollection nvc;
            object value;
            if (req.Properties.TryGetValue("Raven.QueryString", out value))
            {
                nvc = (NameValueCollection) value;
                return nvc[key];
            }
            nvc = HttpUtility.ParseQueryString(req.RequestUri.Query);

            if (!ClientIsV3OrHigher(req))
            {
                var originalQuery = nvc["query"];
                if (originalQuery != null)
                    nvc["query"] = originalQuery.Replace("+", "%2B");
                foreach (var queryKey in nvc.AllKeys)
                    nvc[queryKey] = UnescapeStringIfNeeded(nvc[queryKey]);
            }

            req.Properties["Raven.QueryString"] = nvc;
            return nvc[key];
        }

        public string[] GetQueryStringValues(string key)
        {
            var items = InnerRequest.GetQueryNameValuePairs().Where(pair => pair.Key == key);
            return items.Select(pair => (pair.Value != null) ? Uri.UnescapeDataString(pair.Value) : null ).ToArray();
        }

        protected static bool ClientIsV3OrHigher(HttpRequestMessage req)
        {
            IEnumerable<string> values;
            if (req.Headers.TryGetValues("Raven-Client-Version", out values) == false)
                return false; // probably 1.0 client?
            foreach (var value in values)
            {
                if (string.IsNullOrEmpty(value)) return false;
                if (value[0] == '1' || value[0] == '2') return false;
            }
            return true;
        }

        public Etag GetEtagFromQueryString()
        {
            var etagAsString = GetQueryStringValue("etag");
            return etagAsString != null ? Etag.Parse(etagAsString) : null;
        }

        public void WriteETag(Etag etag, HttpResponseMessage msg)
        {
            if (etag == null)
                return;
            WriteETag(etag.ToString(), msg);
        }

        public void WriteETag(string etag, HttpResponseMessage msg)
        {
            if (string.IsNullOrWhiteSpace(etag))
                return;

            msg.Headers.ETag = new EntityTagHeaderValue("\"" + etag + "\"");
        }

        public void WriteHeaders(RavenJObject headers, Etag etag, HttpResponseMessage msg)
        {
            foreach (var header in headers)
            {
                if (header.Key.StartsWith("@"))
                    continue;

                switch (header.Key)
                {
                    case "Content-Type":
                        var headerValue = header.Value.Value<string>();
                        string charset = null;
                        if (headerValue.Contains("charset="))
                        {
                            var splits = headerValue.Split(';');
                            headerValue = splits[0];

                            charset = splits[1].Split('=')[1];
                        }

                        msg.Content.Headers.ContentType = new MediaTypeHeaderValue(headerValue) { CharSet = charset };

                        break;
                    default:
                        if (header.Value.Type == JTokenType.Date)
                        {
                            if (header.Key.StartsWith("Raven-"))
                            {
                                var iso8601 = GetDateString(header.Value, "o");
                                msg.Content.Headers.Add(header.Key, iso8601);
                            }
                            else
                            {
                                var rfc1123 = GetDateString(header.Value, "r");
                                msg.Content.Headers.Add(header.Key, rfc1123);
                                if (!headers.ContainsKey("Raven-" + header.Key))
                                {
                                    var iso8601 = GetDateString(header.Value, "o");
                                    msg.Content.Headers.Add("Raven-" + header.Key, iso8601);
                                }                                    
                            }
                        }
                        else if (header.Value.Type == JTokenType.Boolean)
                        {
                            msg.Content.Headers.Add(header.Key, header.Value.ToString());
                        }
                        else
                        {
                            //headers do not need url decoding because they might contain special symbols (like + symbol in clr type)
                            var value = UnescapeStringIfNeeded(header.Value.ToString(Formatting.None), shouldDecodeUrl: false);
                            msg.Content.Headers.Add(header.Key, value);
                        }
                        break;
                }
            }
            if (headers["@Http-Status-Code"] != null)
            {
                msg.StatusCode = (HttpStatusCode)headers.Value<int>("@Http-Status-Code");
                msg.Content.Headers.Add("Temp-Status-Description", headers.Value<string>("@Http-Status-Description"));
            }

            WriteETag(etag, msg);
        }

        public void AddHeader(string key, string value, HttpResponseMessage msg)
        {
            if (msg.Content == null)
                msg.Content = JsonContent();

            // Ensure we haven't already appended these values.
            IEnumerable<string> existingValues;
            var hasExistingHeaderAppended = msg.Content.Headers.TryGetValues(key, out existingValues) && existingValues.Any(v => v == value);
            if (!hasExistingHeaderAppended)
            {
                msg.Content.Headers.Add(key, value);
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

        private static string UnescapeStringIfNeeded(string str, bool shouldDecodeUrl = true)
        {
            if (str.StartsWith("\"") && str.EndsWith("\""))
                str = Regex.Unescape(str.Substring(1, str.Length - 2));
            if (str.Any(ch => ch > 127))
            {
                // contains non ASCII chars, needs encoding
                return Uri.EscapeDataString(str);
            }

            return shouldDecodeUrl ? HttpUtility.UrlDecode(str) : str;
        }

        public virtual HttpResponseMessage GetMessageWithObject(object item, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            var token = item as RavenJToken;
            if (token == null && item != null)
            {
                token = RavenJToken.FromObject(item);
            }

            bool metadataOnly;
            if (bool.TryParse(GetQueryStringValue("metadata-only"), out metadataOnly) && metadataOnly)
                token = Extensions.HttpExtensions.MinimizeToken(token);
            
            var msg = new HttpResponseMessage(code)
            {
                Content = JsonContent(token),
            };

            WriteETag(etag, msg);

            return msg;
        }

        public virtual HttpResponseMessage GetMessageWithString(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            var resMsg = new HttpResponseMessage(code)
            {
                Content = new MultiGetSafeStringContent(msg),
            };

            WriteETag(etag, resMsg);

            return resMsg;
        }

        public virtual HttpResponseMessage GetEmptyMessage(HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            var resMsg = new HttpResponseMessage(code)
            {
                Content = JsonContent()
            };
            WriteETag(etag, resMsg);
            return resMsg;
        }

        public virtual Task<HttpResponseMessage> GetMessageWithObjectAsTask(object item, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            return new CompletedTask<HttpResponseMessage>(GetMessageWithObject(item, code, etag));
        }

        public Task<HttpResponseMessage> GetMessageWithStringAsTask(string msg, HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            return new CompletedTask<HttpResponseMessage>(GetMessageWithString(msg, code, etag));
        }

        public Task<HttpResponseMessage> GetEmptyMessageAsTask(HttpStatusCode code = HttpStatusCode.OK, Etag etag = null)
        {
            return new CompletedTask<HttpResponseMessage>(GetEmptyMessage(code, etag));
        }

        public HttpResponseMessage WriteData(RavenJObject data, RavenJObject headers, Etag etag, HttpStatusCode status = HttpStatusCode.OK, HttpResponseMessage msg = null)
        {
            if (msg == null)
                msg = GetEmptyMessage(status);

            var jsonContent = ((JsonContent)msg.Content);

            WriteHeaders(headers, etag, msg);

            var jsonp = GetQueryStringValue("jsonp");
            if (string.IsNullOrEmpty(jsonp) == false)
                jsonContent.Jsonp = jsonp;

            jsonContent.Data = data;

            return msg;
        }

        public Etag GetEtag()
        {
            var etagAsString = GetHeader("If-None-Match") ?? GetHeader("If-Match");
            if (etagAsString != null)
            {
                // etags are usually quoted
                if (etagAsString.StartsWith("\"") && etagAsString.EndsWith("\""))
                    etagAsString = etagAsString.Substring(1, etagAsString.Length - 2);

                Etag result;
                if (Etag.TryParse(etagAsString, out result))
                    return result;

                throw new BadRequestException("Could not parse If-None-Match or If-Match header as Guid");
            }

            return null;
        }

        public string GetHeader(string key)
        {
            IEnumerable<string> values;
            if (InnerRequest.Headers.TryGetValues(key, out values) ||
                (InnerRequest.Content != null && InnerRequest.Content.Headers.TryGetValues(key, out values)))
                return values.FirstOrDefault();
            return null;
        }

        public List<string> GetHeaders(string key)
        {
            IEnumerable<string> values;
            if (InnerRequest.Headers.TryGetValues(key, out values) ||
                InnerRequest.Content.Headers.TryGetValues(key, out values))
                return values.ToList();
            return null;
        }

        public bool HasCookie(string key)
        {
            return InnerRequest.Headers.GetCookies(key).Count != 0;
        }

        public string GetCookie(string key)
        {
            var cookieHeaderValue = InnerRequest.Headers.GetCookies(key).FirstOrDefault();
            if (cookieHeaderValue != null)
            {
                var cookie = cookieHeaderValue.Cookies.FirstOrDefault();
                if (cookie != null)
                    return cookie.Value;
            }

            return null;
        }

        public HttpResponseMessage WriteEmbeddedFile(string ravenPath, string embeddedPath, string zipPath,  string docPath)
        {
            var filePath = Path.Combine(ravenPath, docPath);
            if (File.Exists(filePath))
                return WriteFile(filePath);
            
            filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "../Raven.Studio.Html5/", docPath);
            if (File.Exists(filePath))
                return WriteFile(filePath);

            filePath = Path.Combine(this.SystemConfiguration.EmbeddedFilesDirectory, docPath);
            if (File.Exists(filePath))
                return WriteFile(filePath);

            filePath = Path.Combine("~/../../../../Raven.Studio.Html5", docPath);
            if (File.Exists(filePath))
                return WriteFile(filePath);

            if (string.IsNullOrEmpty(zipPath) == false)
            {
                var fullZipPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, zipPath + ".zip");

                if (File.Exists(fullZipPath) == false)
                    fullZipPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bin", zipPath + ".zip");

                if (File.Exists(fullZipPath) == false)
                    fullZipPath = Path.Combine(this.SystemConfiguration.EmbeddedFilesDirectory, zipPath + ".zip");

                if (File.Exists(fullZipPath))
                {
                    return WriteFileFromZip(fullZipPath, docPath);
                }
            }

            return WriteEmbeddedFileOfType(embeddedPath, docPath);
        }

        private HttpResponseMessage WriteFileFromZip(string zipPath, string docPath)
        {
            var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-Match");
            var currentFileEtag = EmbeddedLastChangedDate + docPath;
            if (etagValue == "\"" + currentFileEtag + "\"")
                return GetEmptyMessage(HttpStatusCode.NotModified);

            var fileStream = new FileStream(zipPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var zipArchive = new ZipArchive(fileStream, ZipArchiveMode.Read, false);
            
            var zipEntry = zipArchive.Entries.FirstOrDefault(a => a.FullName.Equals(docPath, StringComparison.OrdinalIgnoreCase));
            if (zipEntry == null)
                return EmbeddedFileNotFound(docPath);

            var entry = zipEntry.Open();
            var msg = new HttpResponseMessage
            {
                Content = new CompressedStreamContent(entry, false)
                {
                    Disposables = { zipArchive }
                },
            };

            WriteETag(currentFileEtag, msg);

            var type = GetContentType(docPath);
            msg.Content.Headers.ContentType = new MediaTypeHeaderValue(type);

            return msg;
        }

        public HttpResponseMessage WriteFile(string filePath)
        {
            var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-Match");
            if (etagValue != null)
            {
                // Bug fix: the etag header starts and ends with quotes, resulting in cache-busting; the Studio always receives new files, even if should be cached.
                etagValue = etagValue.Trim(new[] { '\"' });
            }

            var fileEtag = File.GetLastWriteTimeUtc(filePath).ToString("G");
            if (etagValue == fileEtag)
                return GetEmptyMessage(HttpStatusCode.NotModified);

            var msg = new HttpResponseMessage
            {
                Content = new CompressedStreamContent(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite), false)
            };

            WriteETag(fileEtag, msg);

            var type = GetContentType(filePath);
            msg.Content.Headers.ContentType = new MediaTypeHeaderValue(type);

            return msg;
        }

        private HttpResponseMessage WriteEmbeddedFileOfType(string embeddedPath, string docPath)
        {
            var etagValue = GetHeader("If-None-Match") ?? GetHeader("If-Match");
            var currentFileEtag = EmbeddedLastChangedDate + docPath;
            if (etagValue == "\"" + currentFileEtag + "\"")
                return GetEmptyMessage(HttpStatusCode.NotModified);

            byte[] bytes;
            var resourceName = embeddedPath + "." + docPath.Replace("/", ".");

            var resourceAssembly = typeof(RavenBaseApiController).Assembly;
            var resourceNames = resourceAssembly.GetManifestResourceNames();
            var lowercasedResourceName = resourceNames.FirstOrDefault(s => string.Equals(s, resourceName, StringComparison.OrdinalIgnoreCase));
            if (lowercasedResourceName == null)
            {
                return EmbeddedFileNotFound(docPath);
            }
            using (var resource = resourceAssembly.GetManifestResourceStream(lowercasedResourceName))
            {
                if (resource == null)
                    return EmbeddedFileNotFound(docPath);

                bytes = resource.ReadData();
            }
            var msg = new HttpResponseMessage
            {
                Content = new ByteArrayContent(bytes),
            };

            WriteETag(currentFileEtag, msg);

            var type = GetContentType(docPath);
            msg.Content.Headers.ContentType = new MediaTypeHeaderValue(type);

            return msg;
        }

        private HttpResponseMessage EmbeddedFileNotFound(string docPath)
        {
            var message = "The following embedded file was not available: " + docPath +
                          ". Please make sure that the Raven.Studio.Html5.zip file exist in the main directory (near to the Raven.Database.dll).";
            return GetMessageWithObject(new {Message = message}, HttpStatusCode.NotFound);
        }

        private static readonly string EmbeddedLastChangedDate =
            File.GetLastWriteTime(AssemblyHelper.GetAssemblyLocationFor(typeof(HttpExtensions))).Ticks.ToString("G");

        private static string GetContentType(string docPath)
        {
            switch (Path.GetExtension(docPath))
            {
                case ".html":
                case ".htm":
                    return "text/html";
                case ".css":
                    return "text/css";
                case ".js":
                    return "text/javascript";
                case ".ico":
                    return "image/vnd.microsoft.icon";
                case ".jpg":
                    return "image/jpeg";
                case ".gif":
                    return "image/gif";
                case ".png":
                    return "image/png";
                case ".xap":
                    return "application/x-silverlight-2";
                case ".json":
                    return "application/json";
                default:
                    return "text/plain";
            }
        }

        protected class Headers : HttpHeaders {}

        public JsonContent JsonContent(RavenJToken data = null)
        {
            return new JsonContent(data)
                .WithRequest(InnerRequest);
        }

        public string GetRequestUrl()
        {
            var rawUrl = InnerRequest.RequestUri.PathAndQuery;
            return UrlExtension.GetRequestUrlFromRawUrl(rawUrl, SystemConfiguration);
        }

        public abstract InMemoryRavenConfiguration SystemConfiguration { get; }


        protected void AddRavenHeader(HttpResponseMessage msg, Stopwatch sp)
        {
            AddHeader(Constants.RavenServerBuild, DocumentDatabase.BuildVersion.ToInvariantString(), msg);
            AddHeader("Temp-Request-Time", sp.ElapsedMilliseconds.ToString("#,#;;0", CultureInfo.InvariantCulture), msg);
        }

        public abstract bool TrySetupRequestToProperResource(out RequestWebApiEventArgs args);

        public abstract string TenantName { get; }

        private int innerRequestsCount;

        public int InnerRequestsCount { get { return innerRequestsCount;  } }

        public List<Action<StringBuilder>> CustomRequestTraceInfo { get; private set; }

        public abstract InMemoryRavenConfiguration ResourceConfiguration { get; }

        public void AddRequestTraceInfo(Action<StringBuilder> info)
        {
            if (info == null)
                return;

            if (CustomRequestTraceInfo == null)
                CustomRequestTraceInfo = new List<Action<StringBuilder>>();

            CustomRequestTraceInfo.Add(info);
        }

        public void IncrementInnerRequestsCount()
        {
            Interlocked.Increment(ref innerRequestsCount);
        }


        public abstract void MarkRequestDuration(long duration);


        #region Landlords

        private DatabasesLandlord landlord;
        public DatabasesLandlord DatabasesLandlord
        {
            get
            {
                if (Configuration == null || landlord != null)
                    return landlord;
                return landlord = (DatabasesLandlord)Configuration.Properties[typeof(DatabasesLandlord)];
            }
        }

        private CountersLandlord countersLandlord;
        public CountersLandlord CountersLandlord
        {
            get
            {
                if (Configuration == null)
                    return countersLandlord;
                return (CountersLandlord)Configuration.Properties[typeof(CountersLandlord)];
            }
        }

        private FileSystemsLandlord fileSystemsLandlord;
        public FileSystemsLandlord FileSystemsLandlord
        {
            get
            {
                if (Configuration == null)
                    return fileSystemsLandlord;
                return (FileSystemsLandlord)Configuration.Properties[typeof(FileSystemsLandlord)];
            }
        }

        private RequestManager requestManager;
        public RequestManager RequestManager
        {
            get
            {
                if (Configuration == null)
                    return requestManager;
                return (RequestManager)Configuration.Properties[typeof(RequestManager)];
            }
        }

        private SemaphoreSlim maxNumberOfThreadsForDatabaseToLoad;
        private int maxSecondsForTaskToWaitForDatabaseToLoad;
        public SemaphoreSlim MaxNumberOfThreadsForDatabaseToLoad
        {
            get
            {
                if (Configuration == null)
                    return maxNumberOfThreadsForDatabaseToLoad;
                return (SemaphoreSlim)Configuration.Properties[Constants.MaxConcurrentRequestsForDatabaseDuringLoad];
            }
        }
        public int MaxSecondsForTaskToWaitForDatabaseToLoad
        {
            get
            {
                if (Configuration == null)
                {
                    return maxSecondsForTaskToWaitForDatabaseToLoad;
                }
                return (int)Configuration.Properties[Constants.MaxSecondsForTaskToWaitForDatabaseToLoad];
            }
        }
        #endregion
    }
}
