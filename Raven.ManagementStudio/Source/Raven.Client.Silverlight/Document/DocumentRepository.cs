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

                request.BeginGetResponse((result) => this.GetMany_Callback(existingEntities, request, result, callback, storeCallback), request);
            }
            else
            {
                this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGetMany, this.DatabaseAddress)), RequestMethod.POST, out request);

                request.BeginGetRequestStream(
                    (requestResult) =>
                    {
                        var writer = new StreamWriter(request.EndGetRequestStream(requestResult));

                        foreach (var key in keys)
                        {
                            writer.Write(key);
                        }

                        writer.Close();

                        request.BeginGetResponse((responseResult) => this.GetMany_Callback(existingEntities, request, responseResult, callback, storeCallback), request);
                    },
                    request);
            }
        }

        public void Put<T>(T entity, CallbackFunction.Save<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
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

        public void Post<T>(T entity, CallbackFunction.Save<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
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

        public void Delete<T>(T entity, CallbackFunction.Save<T> callback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPut, this.DatabaseAddress, entity.Key)), RequestMethod.DELETE, out request);

            request.BeginGetResponse((result) => this.Delete_Callback(entity, request, result, callback), request);
        }

        private void Get_Callback<T>(string key, HttpWebRequest request, IAsyncResult result, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            var context = this.GetContext(request);

            HttpStatusCode statusCode;
            var json = this.GetResponseStream(request, result, out statusCode);

            var document = this.Mapper.Map(json); // TODO
            document.Id = document.Key = key;

            var loadResponse = new LoadResponse<T>()
                                   {
                                       Data = document as T,
                                       StatusCode = statusCode
                                   };

            context.Post(
                delegate
                {
                    callback.Invoke(loadResponse);
                },
                null);

            context.Post(
                delegate
                {
                    storeCallback.Invoke(loadResponse.Data);
                },
                null);
        }

        private void GetMany_Callback<T>(IList<T> existingEntities, HttpWebRequest request, IAsyncResult result, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : JsonDocument
        {
            var context = this.GetContext(request);

            HttpStatusCode statusCode;
            var json = this.GetResponseStream(request, result, out statusCode);
            var array = JArray.Parse(json);

            var entities = array.Select(jsonDocument => (T)this.Mapper.Map(jsonDocument.ToString()));
            var responseResult = existingEntities != null ? entities.Concat(existingEntities).ToList() : entities.ToList();

            var loadResponse = new LoadResponse<IList<T>>()
                                   {
                                       Data = responseResult,
                                       StatusCode = statusCode
                                   };

            context.Post(
                delegate
                {
                    callback.Invoke(loadResponse);
                },
                null);

            context.Post(
                delegate
                {
                    storeCallback.Invoke(loadResponse.Data);
                },
                null);
        }

        private void Save_Callback<T>(T entity, HttpWebRequest request, IAsyncResult result, CallbackFunction.Save<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            var context = this.GetContext(request);

            HttpStatusCode statusCode;
            var json = this.GetResponseStream(request, result, out statusCode);

            var responseJson = JObject.Parse(json);

            entity.Etag = new Guid(responseJson["ETag"].ToString().Replace("\"", string.Empty));
            entity.Key = entity.Id = responseJson["Key"].ToString().Replace("\"", string.Empty);

            var saveResponse = new SaveResponse<T>()
                                   {
                                       Data = entity,
                                       StatusCode = statusCode
                                   };

            context.Post(
                delegate
                {
                    callback.Invoke(saveResponse);
                },
                null);

            context.Post(
                delegate
                {
                    storeCallback.Invoke(saveResponse.Data);
                },
                null);
        }

        private void Delete_Callback<T>(T entity, HttpWebRequest request, IAsyncResult result, CallbackFunction.Save<T> callback) where T : JsonDocument
        {
            var context = this.GetContext(request);

            HttpStatusCode statusCode;
            var json = this.GetResponseStream(request, result, out statusCode);

            var deleteResponse = new DeleteResponse<T>()
                                     {
                                         Data = entity,
                                         StatusCode = statusCode
                                     };

            context.Post(
                delegate
                    {
                        callback.Invoke(deleteResponse);
                    },
                null);
        }
    }
}
