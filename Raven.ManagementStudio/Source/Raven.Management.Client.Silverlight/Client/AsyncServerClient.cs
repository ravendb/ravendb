namespace Raven.Management.Client.Silverlight.Client
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Browser;
    using System.Threading;
    using Abstractions.Data;
    using Common;
    using Common.Converters;
    using Common.Mappers;
    using Database;
    using Database.Data;
    using Database.Indexing;
    using Document;
    using Http.Exceptions;
    using Http.Json;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Access the database commands in async fashion
    /// </summary>
    public class AsyncServerClient : IAsyncDatabaseCommands
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <param name="convention">The convention.</param>
        /// <param name="credentials">The credentials.</param>
        public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials)
        {
            Guard.Assert(() => !string.IsNullOrEmpty(url));
            Guard.Assert(() => convention != null);

            WebRequest.RegisterPrefix("http://", WebRequestCreator.ClientHttp);

            DatabaseAddress = new Uri(url);
            Convention = convention;
            Credentials = credentials;

            ContextStorage = new Dictionary<HttpJsonRequest, SynchronizationContext>();
            DocumentMapper = new DocumentMapper();
            IndexMapper = new IndexMapper(Convention);
            AttachmentMapper = new AttachmentMapper();
        }

        private IDictionary<HttpJsonRequest, SynchronizationContext> ContextStorage { get; set; }

        private Uri DatabaseAddress { get; set; }

        private ICredentials Credentials { get; set; }

        private DocumentConvention Convention { get; set; }

        private IMapper<JsonDocument> DocumentMapper { get; set; }

        private IMapper<KeyValuePair<string, IndexDefinition>> IndexMapper { get; set; }

        private AttachmentMapper AttachmentMapper { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="requestUri"></param>
        /// <param name="method"></param>
        /// <param name="request"></param>
        protected void CreateContext(Uri requestUri, RequestMethod method, out HttpJsonRequest request)
        {
            Guard.Assert(() => requestUri != null);

            var metadata = new JObject();

            request = HttpJsonRequest.CreateHttpJsonRequest(this, new Uri(requestUri.AbsoluteUri + Guid.NewGuid()),
                                                            method, metadata, Credentials);

            ContextStorage.Add(request, SynchronizationContext.Current);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        protected SynchronizationContext GetContext(HttpJsonRequest request)
        {
            Guard.Assert(() => ContextStorage.ContainsKey(request));

            return ContextStorage[request];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        protected void DeleteContext(HttpJsonRequest request)
        {
            Guard.Assert(() => request != null);

            ContextStorage.Remove(request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="result"></param>
        /// <param name="statusCode"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        protected string GetResponseStream(HttpWebRequest request, IAsyncResult result, out HttpStatusCode statusCode,
                                           out Exception exception, out NameValueCollection headers)
        {
            try
            {
                var response = request.EndGetResponse(result) as HttpWebResponse;

                Stream stream = response.GetResponseStream();

                var reader = new StreamReader(stream);

                statusCode = response.StatusCode;
                exception = null;
                headers = response.Headers.ConvertToNameValueCollection();

                return reader.ReadToEnd();
            }
            catch (WebException ex)
            {
                var response = ex.Response as HttpWebResponse;
                if (response == null)
                {
                    throw;
                }

                statusCode = response.StatusCode;
                exception = ExtractException(response);
                headers = response.Headers.ConvertToNameValueCollection();

                return string.Empty;
            }
            catch (Exception ex)
            {
                statusCode = HttpStatusCode.NotImplemented;
                exception = ex;
                headers = null;

                return new JArray().ToString();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        public static Exception ExtractException(HttpWebResponse response)
        {
            Stream stream = response.GetResponseStream();

            var reader = new StreamReader(stream);

            string json = reader.ReadToEnd();

            switch (response.StatusCode)
            {
                case HttpStatusCode.Conflict:
                    var errorResults = JsonConvert.DeserializeAnonymousType(json, new
                                                                                      {
                                                                                          url = (string)null,
                                                                                          actualETag = Guid.Empty,
                                                                                          expectedETag = Guid.Empty,
                                                                                          error = (string)null
                                                                                      });
                    return new ConcurrencyException(errorResults.error)
                               {
                                   ActualETag = errorResults.actualETag,
                                   ExpectedETag = errorResults.expectedETag
                               };
                default:
                    return
                        new Exception(string.IsNullOrEmpty(json)
                                          ? string.Empty
                                          : JObject.Parse(json)["Error"].ToString());
            }
        }

        #region Old Code

        //        /// <summary>
        //        /// Begins the async query.
        //        /// </summary>
        //        /// <param name="index">The index.</param>
        //        /// <param name="query">The query.</param>
        //        /// <param name="callback">The callback.</param>
        //        /// <param name="state">The state.</param>
        //        /// <returns></returns>
        //        public IAsyncResult BeginQuery(string index, IndexQuery query, AsyncCallback callback, object state)
        //        {
        //            EnsureIsNotNullOrEmpty(index, "index");
        //            string path = query.GetIndexQueryUrl(url, index, "indexes");
        //            var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "GET", credentials);

        //            var asyncCallback = callback;
        //            if (callback != null)
        //                asyncCallback = ar => callback(new UserAsyncData(request, ar));

        //            var asyncResult = request.BeginReadResponseString(asyncCallback, state);
        //            return new UserAsyncData(request, asyncResult);
        //        }

        //        /// <summary>
        //        /// Ends the async query.
        //        /// </summary>
        //        /// <param name="result">The result.</param>
        //        /// <returns></returns>
        //        public QueryResult EndQuery(IAsyncResult result)
        //        {
        //            var userAsyncData = ((UserAsyncData)result);
        //            var responseString = userAsyncData.Request.EndReadResponseString(userAsyncData.Result);
        //            JToken json;
        //            using (var reader = new JsonTextReader(new StringReader(responseString)))
        //                json = (JToken)convention.CreateSerializer().Deserialize(reader);

        //            return new QueryResult
        //            {
        //                IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
        //                IndexTimestamp = json.Value<DateTime>("IndexTimestamp"),
        //                Results = json["Results"].Children().Cast<JObject>().ToList(),
        //                TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
        //                SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString())
        //            };
        //        }

        #endregion

        #region Document

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        public void DocumentGet(string key, CallbackFunction.Load<JsonDocument> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGet, DatabaseAddress, key)), RequestMethod.GET,
                          out request);

            request.HttpWebRequest.BeginGetResponse(
                (result) => DocumentGet_Callback(key, request, result, callback), request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="callback"></param>
        public void DocumentGetMany(string[] keys, CallbackFunction.Load<IList<JsonDocument>> callback)
        {
            HttpJsonRequest request;

            if (keys == null || keys.Length == 0)
            {
                CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGetAll, DatabaseAddress)), RequestMethod.GET,
                              out request);

                request.HttpWebRequest.BeginGetResponse(
                    (result) => DocumentGetAll_Callback(request, result, callback), request);
            }
            else
            {
                CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGetMany, DatabaseAddress)), RequestMethod.POST,
                              out request);

                request.HttpWebRequest.BeginGetRequestStream(
                    (requestResult) =>
                    {
                        var writer = new StreamWriter(request.HttpWebRequest.EndGetRequestStream(requestResult));

                        var array = new JArray(keys);

                        writer.Write(array.ToString());
                        writer.Close();

                        request.HttpWebRequest.BeginGetResponse(
                            (responseResult) =>
                            DocumentGetMany_Callback(request, responseResult, callback),
                            request);
                    },
                    request);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="callback"></param>
        public void DocumentPut(JsonDocument entity, CallbackFunction.Save<JsonDocument> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPut, DatabaseAddress, entity.Key)),
                          RequestMethod.PUT, out request);

            request.HttpWebRequest.BeginGetRequestStream(
                (requestResult) =>
                {
                    var writer = new StreamWriter(request.HttpWebRequest.EndGetRequestStream(requestResult));

                    writer.Write(entity.ToJson());
                    writer.Close();

                    request.HttpWebRequest.BeginGetResponse(
                        (responseResult) => DocumentSave_Callback(entity, request, responseResult, callback),
                        request);
                },
                request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="callback"></param>
        public void DocumentPost(JsonDocument entity, CallbackFunction.Save<JsonDocument> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPost, DatabaseAddress)), RequestMethod.POST,
                          out request);

            request.HttpWebRequest.BeginGetRequestStream(
                (requestResult) =>
                {
                    var writer = new StreamWriter(request.HttpWebRequest.EndGetRequestStream(requestResult));

                    writer.Write(entity.ToJson());
                    writer.Close();

                    request.HttpWebRequest.BeginGetResponse(
                        (responseResult) => DocumentSave_Callback(entity, request, responseResult, callback),
                        request);
                },
                request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="callback"></param>
        public void DocumentDelete(JsonDocument entity, CallbackFunction.Save<string> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPut, DatabaseAddress, entity.Key)),
                          RequestMethod.DELETE, out request);

            request.HttpWebRequest.BeginGetResponse(
                (result) => DocumentDelete_Callback(entity, request, result, callback), request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="batchCommands"></param>
        /// <param name="batchCallback"></param>
        public void DocumentBatch(IList<ICommandData> batchCommands, CallbackFunction.Batch batchCallback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.DocumentBatch, DatabaseAddress)), RequestMethod.POST,
                          out request);

            request.HttpWebRequest.BeginGetRequestStream(
                (requestResult) =>
                {
                    var writer = new StreamWriter(request.HttpWebRequest.EndGetRequestStream(requestResult));
                    var array = new JArray(batchCommands.Select(x => x.ToJson()));

                    writer.Write(array.ToString(Formatting.None));
                    writer.Close();

                    request.HttpWebRequest.BeginGetResponse(
                        (responseResult) =>
                        DocumentBatch_Callback(batchCommands, request, responseResult, batchCallback),
                        request);
                },
                request);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
        }

        private void DocumentGet_Callback(string key, HttpJsonRequest request, IAsyncResult result,
                                          CallbackFunction.Load<JsonDocument> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            JsonDocument document = null;
            if (exception == null)
            {
                document = DocumentMapper.Map(json);
                document.Metadata = headers.FilterHeaders(isServerDocument: false);
                document.Key = key;
            }

            var loadResponse = new LoadResponse<JsonDocument>
                                   {
                                       Data = document,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(loadResponse); },
                null);
        }

        private void DocumentGetMany_Callback(HttpJsonRequest request, IAsyncResult result,
                                              CallbackFunction.Load<IList<JsonDocument>> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);
            JObject jObject = JObject.Parse(json);

            var responseResult = new List<JsonDocument>();

            if (exception == null)
            {
                responseResult =
                    jObject["Results"].Select(jsonDocument => DocumentMapper.Map(jsonDocument.ToString())).ToList();
            }

            var loadResponse = new LoadResponse<IList<JsonDocument>>
                                   {
                                       Data = responseResult,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(loadResponse); },
                null);
        }

        private void DocumentGetAll_Callback(HttpJsonRequest request, IAsyncResult result,
                                             CallbackFunction.Load<IList<JsonDocument>> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);
            JArray array = JArray.Parse(json);

            var responseResult = new List<JsonDocument>();

            if (exception == null)
            {
                responseResult = array.Select(jsonDocument => DocumentMapper.Map(jsonDocument.ToString())).ToList();
            }

            var loadResponse = new LoadResponse<IList<JsonDocument>>
                                   {
                                       Data = responseResult,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(loadResponse); },
                null);
        }

        private void DocumentSave_Callback(JsonDocument document, HttpJsonRequest request, IAsyncResult result,
                                           CallbackFunction.Save<JsonDocument> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            if (exception == null)
            {
                JObject responseJson = JObject.Parse(json);

                document.Etag = new Guid(responseJson["ETag"].ToString().Replace("\"", string.Empty));
                document.Key = responseJson["Key"].ToString().Replace("\"", string.Empty);
            }

            var saveResponse = new SaveResponse<JsonDocument>
                                   {
                                       Data = document,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(new List<Response<JsonDocument>> { saveResponse }); },
                null);
        }

        private void DocumentDelete_Callback(JsonDocument document, HttpJsonRequest request, IAsyncResult result,
                                             CallbackFunction.Save<string> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            var deleteResponse = new DeleteResponse<string>
                                     {
                                         Data = document.Key,
                                         StatusCode = statusCode,
                                         Exception = exception
                                     };

            context.Post(
                delegate { callback.Invoke(new List<Response<string>> { deleteResponse }); },
                null);
        }

        private void DocumentBatch_Callback(IList<ICommandData> batchCommands, HttpJsonRequest request,
                                            IAsyncResult result, CallbackFunction.Batch batchCallback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            var batchResults = JsonConvert.DeserializeObject<BatchResult[]>(json);

            context.Post(
                delegate { batchCallback.Invoke(batchResults.ToList()); },
                null);
        }

        #endregion

        #region Index

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="callback"></param>
        public void IndexGet(string name, CallbackFunction.Load<KeyValuePair<string, IndexDefinition>> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.IndexGet, DatabaseAddress, name)), RequestMethod.GET,
                          out request);

            request.HttpWebRequest.BeginGetResponse((result) => IndexGet_Callback(name, request, result, callback),
                                                    request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="names"></param>
        /// <param name="existingEntities"></param>
        /// <param name="callback"></param>
        public void IndexGetMany(string[] names, IDictionary<string, IndexDefinition> existingEntities,
                                 CallbackFunction.Load<IDictionary<string, IndexDefinition>> callback)
        {
            HttpJsonRequest request;

            if (names == null || names.Length == 0)
            {
                CreateContext(new Uri(string.Format(DatabaseUrl.IndexGetAll, DatabaseAddress)), RequestMethod.GET,
                              out request);

                request.HttpWebRequest.BeginGetResponse(
                    (result) => IndexGetAll_Callback(existingEntities, request, result, callback), request);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="entity"></param>
        /// <param name="callback"></param>
        public void IndexPut(string name, IndexDefinition entity,
                             CallbackFunction.Save<KeyValuePair<string, IndexDefinition>> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.IndexPut, DatabaseAddress, name)), RequestMethod.PUT,
                          out request);

            request.HttpWebRequest.BeginGetRequestStream(
                (requestResult) =>
                {
                    var writer = new StreamWriter(request.HttpWebRequest.EndGetRequestStream(requestResult));

                    writer.Write(JsonConvert.SerializeObject(entity, new JsonEnumConverter()));
                    writer.Close();

                    request.HttpWebRequest.BeginGetResponse(
                        (responseResult) => IndexSave_Callback(name, entity, request, responseResult, callback),
                        request);
                },
                request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="callback"></param>
        public void IndexDelete(string name, CallbackFunction.Save<string> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.IndexPut, DatabaseAddress, name)), RequestMethod.DELETE,
                          out request);

            request.HttpWebRequest.BeginGetResponse((result) => IndexDelete_Callback(name, request, result, callback),
                                                    request);
        }

        private void IndexGet_Callback(string name, HttpJsonRequest request, IAsyncResult result,
                                       CallbackFunction.Load<KeyValuePair<string, IndexDefinition>> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            var index = new KeyValuePair<string, IndexDefinition>();
            if (exception == null)
            {
                index = IndexMapper.Map(json);
            }

            var loadResponse = new LoadResponse<KeyValuePair<string, IndexDefinition>>
                                   {
                                       Data = index,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(loadResponse); },
                null);
        }

        private void IndexGetAll_Callback(IDictionary<string, IndexDefinition> existingEntities, HttpJsonRequest request,
                                          IAsyncResult result,
                                          CallbackFunction.Load<IDictionary<string, IndexDefinition>> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);
            JArray array = JArray.Parse(json);

            var responseResult = new Dictionary<string, IndexDefinition>();
            if (exception == null)
            {
                IEnumerable<KeyValuePair<string, IndexDefinition>> entities =
                    array.Select(index => IndexMapper.Map(index.ToString()));
                responseResult.Concat(entities);

                if (existingEntities != null)
                {
                    responseResult.Concat(existingEntities);
                }
            }

            var loadResponse = new LoadResponse<IDictionary<string, IndexDefinition>>
                                   {
                                       Data = responseResult,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(loadResponse); },
                null);
        }

        private void IndexSave_Callback(string name, IndexDefinition entity, HttpJsonRequest request,
                                        IAsyncResult result,
                                        CallbackFunction.Save<KeyValuePair<string, IndexDefinition>> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            var loadResponse = new LoadResponse<KeyValuePair<string, IndexDefinition>>
                                   {
                                       Data = new KeyValuePair<string, IndexDefinition>(name, entity),
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(new List<Response<KeyValuePair<string, IndexDefinition>>> { loadResponse }); },
                null);
        }

        private void IndexDelete_Callback(string name, HttpJsonRequest request, IAsyncResult result,
                                          CallbackFunction.Save<string> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            var deleteResponse = new DeleteResponse<string>
                                     {
                                         Data = name,
                                         StatusCode = statusCode,
                                         Exception = exception
                                     };

            context.Post(
                delegate { callback.Invoke(new List<Response<string>> { deleteResponse }); },
                null);
        }

        #endregion

        #region Attachment

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        public void AttachmentGet(string key, CallbackFunction.Load<KeyValuePair<string, Attachment>> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.Attachment, DatabaseAddress, key)), RequestMethod.GET,
                          out request);

            request.HttpWebRequest.BeginGetResponse((result) => AttachmentGet_Callback(key, request, result, callback),
                                                    request);
        }

        public void AttachmentGetAll(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.AttachmentGetAll, DatabaseAddress)), RequestMethod.GET,
                          out request);

            request.HttpWebRequest.BeginGetResponse((result) => AttachmentGetAll_Callback(request, result, callback),
                                                    request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        public void AttachmentDelete(string key, CallbackFunction.Save<string> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.Attachment, DatabaseAddress, key)), RequestMethod.DELETE,
                          out request);

            request.HttpWebRequest.BeginGetResponse(
                (result) => AttachmentDelete_Callback(key, request, result, callback), request);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="etag"></param>
        /// <param name="data"></param>
        /// <param name="metadata"></param>
        /// <param name="callback"></param>
        public void AttachmentPut(string key, Guid? etag, byte[] data, JObject metadata,
                                  CallbackFunction.Save<Attachment> callback)
        {
            HttpJsonRequest request;
            CreateContext(new Uri(string.Format(DatabaseUrl.Attachment, DatabaseAddress, key)), RequestMethod.PUT,
                          out request);

            foreach (JProperty header in metadata.Properties())
            {
                if (header.Name.StartsWith("@"))
                    continue;

                string matchString = header.Name;
                string formattedHeaderValue = StripQuotesIfNeeded(header.Value.ToString(Formatting.None));

                switch (matchString)
                {
                    case "Content-Length":
                        break;
                    case "Content-Type":
                        request.HttpWebRequest.ContentType = formattedHeaderValue;
                        break;
                    default:
                        request.HttpWebRequest.Headers[header.Name] = formattedHeaderValue;
                        break;
                }
            }

            if (etag != null)
            {
                request.HttpWebRequest.Headers[" If-None-Match"] = etag.Value.ToString();
            }

            request.HttpWebRequest.BeginGetRequestStream((requestResult) =>
                                                             {
                                                                 var writer = new BinaryWriter(request.HttpWebRequest.EndGetRequestStream(requestResult));

                                                                 writer.Write(data);
                                                                 writer.Close();

                                                                 request.HttpWebRequest.BeginGetResponse(
                                                                     (responseResult) =>
                                                                     AttachmentPut_Callback(key, etag, data, metadata,
                                                                                            request, responseResult,
                                                                                            callback),
                                                                     request);
                                                             },
                                                         null);
        }

        private static string StripQuotesIfNeeded(string str)
        {
            if (str.StartsWith("\"") && str.EndsWith("\""))
                return str.Substring(1, str.Length - 2);
            return str;
        }

        private void AttachmentGet_Callback(string key, HttpJsonRequest request, IAsyncResult result,
                                            CallbackFunction.Load<KeyValuePair<string, Attachment>> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;

            Attachment attachment = AttachmentMapper.Map(key, request.HttpWebRequest, result, out statusCode,
                                                         out exception);

            var loadResponse = new LoadResponse<KeyValuePair<string, Attachment>>()
                                   {
                                       Data = new KeyValuePair<string, Attachment>(key, attachment),
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(loadResponse); },
                null);
        }

        private void AttachmentGetAll_Callback(HttpJsonRequest request, IAsyncResult result, CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;

            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            JArray array = JArray.Parse(json);

            var attachments = new List<KeyValuePair<string, Attachment>>();
            string key;

            foreach (var attachment in array)
            {
                attachments.Add(AttachmentMapper.Map(attachment));
            }

            var loadResponse = new LoadResponse<IList<KeyValuePair<string, Attachment>>>
                                   {
                                       Data = attachments,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(loadResponse); },
                null);
        }

        private void AttachmentDelete_Callback(string key, HttpJsonRequest request, IAsyncResult result,
                                               CallbackFunction.Save<string> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            var deleteResponse = new DeleteResponse<string>
                                     {
                                         Data = key,
                                         StatusCode = statusCode,
                                         Exception = exception
                                     };

            context.Post(
                delegate { callback.Invoke(new List<Response<string>> { deleteResponse }); },
                null);
        }

        private void AttachmentPut_Callback(string key, Guid? etag, byte[] data, JObject metadata,
                                            HttpJsonRequest request, IAsyncResult result,
                                            CallbackFunction.Save<Attachment> callback)
        {
            SynchronizationContext context = GetContext(request);
            DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            NameValueCollection headers;
            string json = GetResponseStream(request.HttpWebRequest, result, out statusCode, out exception, out headers);

            var saveResponse = new SaveResponse<Attachment>
                                   {
                                       Data = new Attachment
                                                  {
                                                      Data = data,
                                                      Metadata = metadata
                                                  },
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate { callback.Invoke(new List<Response<Attachment>> { saveResponse }); },
                null);
        }

        #endregion
    }
}