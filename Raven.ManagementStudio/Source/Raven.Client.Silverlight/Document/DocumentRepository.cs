namespace Raven.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Browser;
    using Newtonsoft.Json.Linq;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Common.Helpers;
    using Raven.Client.Silverlight.Common.Mappers;
    using Raven.Client.Silverlight.Data;

    public class DocumentRepository : IDocumentRepository
    {
        public DocumentRepository(Uri databaseAddress)
        {
            WebRequest.RegisterPrefix("http://", WebRequestCreator.ClientHttp);

            this.ContextStorage = new Dictionary<HttpWebRequest, IRepositoryContext>();
            this.DatabaseAddress = databaseAddress;

            this.DocumentMapper = new DocumentMapper();
        }

        private IDictionary<HttpWebRequest, IRepositoryContext> ContextStorage { get; set; }

        private Uri DatabaseAddress { get; set; }

        private DocumentMapper DocumentMapper { get; set; }

        public void Get<T>(string key, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGet, this.DatabaseAddress, key)), RequestMethod.GET, callback, storeCallback, key, out request);

            request.BeginGetResponse(Get_Callback<T>, request);
        }

        public void GetMany<T>(string[] keys, string[] existingIds, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;

            if (keys == null || keys.Length == 0)
            {
                this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGetAll, this.DatabaseAddress)), RequestMethod.GET, callback, storeCallback, null, out request);

                request.BeginGetResponse(GetMany_Callback<T>, request);
            }
            else
            {
                this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentGetMany, this.DatabaseAddress)), RequestMethod.POST, callback, storeCallback, keys, out request);

                request.BeginGetRequestStream(GetMany_Request<T>, request);
            }
        }

        public void Put<T>(T entity, CallbackFunction.Save callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPut, this.DatabaseAddress, entity.Key)), RequestMethod.PUT, callback, storeCallback, entity, out request);

            request.BeginGetRequestStream(Save_Request<T>, request);
        }

        public void Post<T>(T entity, CallbackFunction.Save callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.DocumentPost, this.DatabaseAddress)), RequestMethod.POST, callback, storeCallback, entity, out request);

            request.BeginGetRequestStream(Save_Request<T>, request);
        }

        public void Delete<T>(T entity, CallbackFunction.Save callback) where T : JsonDocument
        {
            HttpWebRequest request;
            this.CreateContext<T>(new Uri(string.Format(DatabaseUrl.DocumentPut, this.DatabaseAddress, entity.Key)), RequestMethod.DELETE, callback, null, entity, out request);

            request.BeginGetResponse(Delete_Callback<T>, request);
        }

        private void CreateContext<T>(Uri requestUri, RequestMethod method, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback, object state, out HttpWebRequest request) where T : JsonDocument
        {
            request = (HttpWebRequest)WebRequest.Create(requestUri);
            request.Method = method.GetName();

            this.ContextStorage.Add(request, new DocumentRepositoryContext<T>(callback, storeCallback, state));
        }

        private void CreateContext<T>(Uri requestUri, RequestMethod method, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback, object state, out HttpWebRequest request) where T : JsonDocument
        {
            request = (HttpWebRequest)WebRequest.Create(requestUri);
            request.Method = method.GetName();

            this.ContextStorage.Add(request, new DocumentRepositoryContext<T>(callback, storeCallback, state));
        }

        private void CreateContext<T>(Uri requestUri, RequestMethod method, CallbackFunction.Save callback, CallbackFunction.Store<T> storeCallback, object state, out HttpWebRequest request) where T : JsonDocument
        {
            request = (HttpWebRequest)WebRequest.Create(requestUri);
            request.Method = method.GetName();

            this.ContextStorage.Add(request, new DocumentRepositoryContext<T>(callback, storeCallback, state));
        }

        private DocumentRepositoryContext<T> GetContext<T>(HttpWebRequest request) where T : JsonDocument
        {
            Guard.Assert(() => this.ContextStorage.ContainsKey(request));

            var context = this.ContextStorage[request] as DocumentRepositoryContext<T>;

            Guard.Assert(() => context != null);

            return context;
        }

        private void Get_Callback<T>(IAsyncResult result) where T : JsonDocument
        {
            var request = result.AsyncState as HttpWebRequest;

            var context = this.GetContext<T>(request);

            var response = request.EndGetResponse(result) as HttpWebResponse;
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream);

            var json = reader.ReadToEnd();

            var document = this.DocumentMapper.Map(json); // TODO
            document.Id = context.State as string;
            document.Key = context.State as string;

            context.Post(document);
        }

        private void GetMany_Request<T>(IAsyncResult result) where T : JsonDocument
        {
            var request = result.AsyncState as HttpWebRequest;
            var writer = new StreamWriter(request.EndGetRequestStream(result));

            var context = this.GetContext<T>(request);

            var keys = context.State as string[];

            foreach (var key in keys)
            {
                writer.Write(key);
            }

            writer.Close();

            request.BeginGetResponse(GetMany_Callback<T>, request);
        }

        private void GetMany_Callback<T>(IAsyncResult result) where T : JsonDocument
        {
            var request = result.AsyncState as HttpWebRequest;

            var context = this.GetContext<T>(request);

            var response = request.EndGetResponse(result) as HttpWebResponse;
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream);

            var json = reader.ReadToEnd();
            var array = JArray.Parse(json);

            var entities = new List<object>();
            foreach (var jsonDocument in array)
            {
                entities.Add(this.DocumentMapper.Map(jsonDocument.ToString()));
            }

            context.Post(entities);
        }

        private void Save_Request<T>(IAsyncResult result) where T : JsonDocument
        {
            var request = result.AsyncState as HttpWebRequest;
            var writer = new StreamWriter(request.EndGetRequestStream(result));

            var context = this.GetContext<T>(request);

            var document = context.State as T;

            writer.Write(document.ToJson());
            writer.Close();

            request.BeginGetResponse(Save_Callback<T>, request);
        }

        private void Save_Callback<T>(IAsyncResult result) where T : JsonDocument
        {
            var request = result.AsyncState as HttpWebRequest;

            var context = this.GetContext<T>(request);

            var entity = context.State as T;

            var saveResponse = new SaveResponse();
            saveResponse.Entity = entity;

            try
            {
                var response = request.EndGetResponse(result) as HttpWebResponse;
                var stream = response.GetResponseStream();
                var reader = new StreamReader(stream);

                var json = reader.ReadToEnd();
                var responseJson = JObject.Parse(json);

                entity.Etag = new Guid(responseJson["ETag"].ToString().Replace("\"", string.Empty));
                entity.Key = responseJson["Key"].ToString().Replace("\"", string.Empty);
                entity.Id = entity.Key;

                saveResponse.StatusCode = response.StatusCode;
            }
            catch (WebException ex)
            {
                var response = ex.Response as HttpWebResponse;
                if (response == null)
                {
                    throw;
                }

                saveResponse.StatusCode = response.StatusCode;
            }

            context.Post(saveResponse);
        }

        private void Delete_Callback<T>(IAsyncResult result) where T : JsonDocument
        {
            var request = result.AsyncState as HttpWebRequest;

            var context = this.GetContext<T>(request);

            var response = request.EndGetResponse(result) as HttpWebResponse;
            var entity = context.State as T;

            var saveResponse = new SaveResponse();
            saveResponse.Entity = entity;
            saveResponse.StatusCode = response.StatusCode;

            context.Post(saveResponse);
        }
    }
}
