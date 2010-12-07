namespace Raven.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Browser;
    using System.Threading;
    using Newtonsoft.Json.Linq;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Common.Mappers;
    using Raven.Client.Silverlight.Data;

    public class DocumentRepository : BaseRepository<JsonDocument>, IDocumentRepository
    {
        public DocumentRepository(Uri databaseAddress)
        {
            WebRequest.RegisterPrefix("http://", WebRequestCreator.ClientHttp);

            this.ContextStorage = new Dictionary<HttpWebRequest, SynchronizationContext>();
            this.DatabaseAddress = databaseAddress;

            this.Mapper = new DocumentMapper();
        }

        public void Get<T>(string key, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGet, this.DatabaseAddress, key)), RequestMethod.GET, out request);

            request.BeginGetResponse((result) => this.Get_Callback(key, request, result, callback, storeCallback), request);
        }

        public void GetMany<T>(string[] keys, IList<T> existingEntities, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;

            if (keys == null || keys.Length == 0)
            {
                this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGetAll, this.DatabaseAddress)), RequestMethod.GET, out request);

                request.BeginGetResponse((result) => this.GetAll_Callback(existingEntities, request, result, callback, storeCallback), request);
            }
            else
            {
                this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGetMany, this.DatabaseAddress)), RequestMethod.POST, out request);

                request.BeginGetRequestStream(
                    (requestResult) =>
                    {
                        var writer = new StreamWriter(request.EndGetRequestStream(requestResult));

                        var array = new JArray(keys);

                        writer.Write(array.ToString());
                        writer.Close();

                        request.BeginGetResponse((responseResult) => this.GetMany_Callback(existingEntities, request, responseResult, callback, storeCallback), request);
                    },
                    request);
            }
        }

        public void Put<T>(T entity, CallbackFunction.Save<IList<T>> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPut, this.DatabaseAddress, entity.Key)), RequestMethod.PUT, out request);

            request.BeginGetRequestStream(
                (requestResult) =>
                {
                    var writer = new StreamWriter(request.EndGetRequestStream(requestResult));

                    writer.Write(entity.ToJson());
                    writer.Close();

                    request.BeginGetResponse((responseResult) => this.Save_Callback(entity, request, responseResult, callback, storeCallback), request);
                },
                request);
        }

        public void Post<T>(T entity, CallbackFunction.Save<IList<T>> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPost, this.DatabaseAddress)), RequestMethod.POST, out request);

            request.BeginGetRequestStream(
                (requestResult) =>
                {
                    var writer = new StreamWriter(request.EndGetRequestStream(requestResult));

                    writer.Write(entity.ToJson());
                    writer.Close();

                    request.BeginGetResponse((responseResult) => this.Save_Callback(entity, request, responseResult, callback, storeCallback), request);
                },
                request);
        }

        public void Delete<T>(T entity, CallbackFunction.Save<IList<T>> callback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPut, this.DatabaseAddress, entity.Key)), RequestMethod.DELETE, out request);

            request.BeginGetResponse((result) => this.Delete_Callback(entity, request, result, callback), request);
        }

        public void Batch<T>(IList<BatchCommand<T>> batchCommands, CallbackFunction.Save<IList<T>> callback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentBatch, this.DatabaseAddress)), RequestMethod.POST, out request);

            request.BeginGetRequestStream(
                (requestResult) =>
                {
                    var writer = new StreamWriter(request.EndGetRequestStream(requestResult));
                    var array = new JArray();

                    foreach (var batchCommand in batchCommands)
                    {
                        var jObject = new JObject();
                        jObject["Method"] = batchCommand.Method.GetName();
                        jObject["Document"] = batchCommand.Entity.DataAsJson == null ? new JObject().ToString() : batchCommand.Entity.DataAsJson.ToString();
                        jObject["Metadata"] = batchCommand.Entity.Metadata == null ? new JObject().ToString() : batchCommand.Entity.Metadata.ToString();
                        jObject["Key"] = batchCommand.Entity.Key;

                        array.Add(jObject);
                    }

                    writer.Write(array.ToString());
                    writer.Close();

                    request.BeginGetResponse((responseResult) => this.Batch_Callback(batchCommands, request, responseResult, callback), request);
                },
                request);
        }

        private void Get_Callback<T>(string key, HttpWebRequest request, IAsyncResult result, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            var context = this.GetContext(request);
            this.DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            var json = this.GetResponseStream(request, result, out statusCode, out exception);

            var document = this.Mapper.Map(json); // TODO
            document.Id = document.Key = key;

            var loadResponse = new LoadResponse<T>()
                                   {
                                       Data = document as T,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate
                {
                    callback.Invoke(loadResponse);
                },
                null);

            if (loadResponse.IsSuccess)
            {
                context.Post(
                    delegate
                    {
                        storeCallback.Invoke(loadResponse.Data);
                    },
                    null);
            }
        }

        private void GetMany_Callback<T>(IList<T> existingEntities, HttpWebRequest request, IAsyncResult result, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : JsonDocument
        {
            var context = this.GetContext(request);
            this.DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            var json = this.GetResponseStream(request, result, out statusCode, out exception);
            var jObject = JObject.Parse(json);

            var entities = jObject["Results"].Select(jsonDocument => (T)this.Mapper.Map(jsonDocument.ToString()));
            var responseResult = existingEntities != null ? entities.Concat(existingEntities).ToList() : entities.ToList();

            var loadResponse = new LoadResponse<IList<T>>()
            {
                Data = responseResult,
                StatusCode = statusCode,
                Exception = exception
            };

            context.Post(
                delegate
                {
                    callback.Invoke(loadResponse);
                },
                null);

            if (loadResponse.IsSuccess)
            {
                context.Post(
                    delegate
                    {
                        storeCallback.Invoke(loadResponse.Data);
                    },
                    null);
            }
        }

        private void GetAll_Callback<T>(IList<T> existingEntities, HttpWebRequest request, IAsyncResult result, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : JsonDocument
        {
            var context = this.GetContext(request);
            this.DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            var json = this.GetResponseStream(request, result, out statusCode, out exception);
            var array = JArray.Parse(json);

            var entities = array.Select(jsonDocument => (T)this.Mapper.Map(jsonDocument.ToString()));
            var responseResult = existingEntities != null ? entities.Concat(existingEntities).ToList() : entities.ToList();

            var loadResponse = new LoadResponse<IList<T>>()
                                   {
                                       Data = responseResult,
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate
                {
                    callback.Invoke(loadResponse);
                },
                null);

            if (loadResponse.IsSuccess)
            {
                context.Post(
                    delegate
                    {
                        storeCallback.Invoke(loadResponse.Data);
                    },
                    null);
            }
        }

        private void Save_Callback<T>(T entity, HttpWebRequest request, IAsyncResult result, CallbackFunction.Save<IList<T>> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            var context = this.GetContext(request);
            this.DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            var json = this.GetResponseStream(request, result, out statusCode, out exception);

            var responseJson = JObject.Parse(json);

            entity.Etag = new Guid(responseJson["ETag"].ToString().Replace("\"", string.Empty));
            entity.Key = entity.Id = responseJson["Key"].ToString().Replace("\"", string.Empty);

            var saveResponse = new SaveResponse<IList<T>>()
                                   {
                                       Data = new List<T>() { entity },
                                       StatusCode = statusCode,
                                       Exception = exception
                                   };

            context.Post(
                delegate
                {
                    callback.Invoke(saveResponse);
                },
                null);

            if (saveResponse.IsSuccess)
            {
                context.Post(
                    delegate
                    {
                        storeCallback.Invoke(entity);
                    },
                    null);
            }
        }

        private void Delete_Callback<T>(T entity, HttpWebRequest request, IAsyncResult result, CallbackFunction.Save<IList<T>> callback) where T : JsonDocument
        {
            var context = this.GetContext(request);
            this.DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            var json = this.GetResponseStream(request, result, out statusCode, out exception);

            var deleteResponse = new DeleteResponse<IList<T>>()
                                     {
                                         Data = new List<T>() { entity },
                                         StatusCode = statusCode,
                                         Exception = exception
                                     };

            context.Post(
                delegate
                {
                    callback.Invoke(deleteResponse);
                },
                null);
        }

        private void Batch_Callback<T>(IList<BatchCommand<T>> batchCommands, HttpWebRequest request, IAsyncResult result, CallbackFunction.Save<IList<T>> callback) where T : JsonDocument
        {
            var context = this.GetContext(request);
            this.DeleteContext(request);

            HttpStatusCode statusCode;
            Exception exception;
            var json = this.GetResponseStream(request, result, out statusCode, out exception);
            var array = JArray.Parse(json);

            foreach (var batchResponse in array)
            {
                var etag = batchResponse["Etag"].ToString().Replace("\"", string.Empty);
                var method = batchResponse["Method"].ToString().Replace("\"", string.Empty);
                var key = batchResponse["Key"].ToString().Replace("\"", string.Empty);
            }

            var saveResponse = new SaveResponse<IList<T>>()
            {
                Data = new List<T>(batchCommands.Select(x => x.Entity)),
                StatusCode = statusCode,
                Exception = exception
            };

            context.Post(
                delegate
                {
                    callback.Invoke(saveResponse);
                },
                null);
        }
    }
}
