namespace Raven.Client.Silverlight.Index
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

    public class IndexRepository : IIndexRepository
    {
        public IndexRepository(Uri databaseAddress)
        {
            WebRequest.RegisterPrefix("http://", WebRequestCreator.ClientHttp);

            this.ContextStorage = new Dictionary<HttpWebRequest, IRepositoryContext>();
            this.DatabaseAddress = databaseAddress;

            this.IndexMapper = new IndexMapper();
        }

        private IDictionary<HttpWebRequest, IRepositoryContext> ContextStorage { get; set; }

        private Uri DatabaseAddress { get; set; }

        private IndexMapper IndexMapper { get; set; }

        public void Get<T>(string name, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : Data.Index
        {
            throw new NotImplementedException();
        }

        public void GetMany<T>(string[] names, string[] existingNames, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : Data.Index
        {
            HttpWebRequest request;

            if (names == null || names.Length == 0)
            {
                this.CreateContext(new Uri(string.Format(DatabaseUrl.IndexGetAll, this.DatabaseAddress)), RequestMethod.GET, callback, storeCallback, null, out request);

                request.BeginGetResponse(GetMany_Callback<T>, request);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void Put<T>(T entity, CallbackFunction.Save callback, CallbackFunction.Store<T> storeCallback) where T : Data.Index
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.IndexPut, this.DatabaseAddress, entity.Name)), RequestMethod.PUT, callback, storeCallback, entity, out request);

            request.BeginGetRequestStream(Save_Request<T>, request);
        }

        public void Delete<T>(T entity, CallbackFunction.Save callback) where T : Data.Index
        {
            HttpWebRequest request;
            this.CreateContext<T>(new Uri(string.Format(DatabaseUrl.IndexPut, this.DatabaseAddress, entity.Name)), RequestMethod.DELETE, callback, null, entity, out request);

            request.BeginGetResponse(Delete_Callback<T>, request);
        }

        private void CreateContext<T>(Uri requestUri, RequestMethod method, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback, object state, out HttpWebRequest request) where T : Data.Index
        {
            request = (HttpWebRequest)WebRequest.Create(new Uri(requestUri.AbsoluteUri + "?" + Guid.NewGuid())); // TODO [ppekrol] workaround for caching problem in SL
            request.Method = method.GetName();

            this.ContextStorage.Add(request, new IndexRepositoryContext<T>(callback, storeCallback, state));
        }

        private void CreateContext<T>(Uri requestUri, RequestMethod method, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback, object state, out HttpWebRequest request) where T : Data.Index
        {
            request = (HttpWebRequest)WebRequest.Create(new Uri(requestUri.AbsoluteUri + "?" + Guid.NewGuid())); // TODO [ppekrol] workaround for caching problem in SL
            request.Method = method.GetName();

            this.ContextStorage.Add(request, new IndexRepositoryContext<T>(callback, storeCallback, state));
        }

        private void CreateContext<T>(Uri requestUri, RequestMethod method, CallbackFunction.Save callback, CallbackFunction.Store<T> storeCallback, object state, out HttpWebRequest request) where T : Data.Index
        {
            request = (HttpWebRequest)WebRequest.Create(new Uri(requestUri.AbsoluteUri + "?" + Guid.NewGuid())); // TODO [ppekrol] workaround for caching problem in SL
            request.Method = method.GetName();

            this.ContextStorage.Add(request, new IndexRepositoryContext<T>(callback, storeCallback, state));
        }

        private IndexRepositoryContext<T> GetContext<T>(HttpWebRequest request) where T : Data.Index
        {
            Guard.Assert(() => this.ContextStorage.ContainsKey(request));

            var context = this.ContextStorage[request] as IndexRepositoryContext<T>;

            Guard.Assert(() => context != null);

            return context;
        }

        private void DeleteContext<T>(HttpWebRequest request) where T : Data.Index
        {
            Guard.Assert(() => request != null);

            this.ContextStorage.Remove(request);
        }

        private void GetMany_Request<T>(IAsyncResult result) where T : Data.Index
        {
            var request = result.AsyncState as HttpWebRequest;
            var writer = new StreamWriter(request.EndGetRequestStream(result));

            var context = this.GetContext<T>(request);

            var names = context.State as string[];

            foreach (var name in names)
            {
                writer.Write(name);
            }

            writer.Close();

            request.BeginGetResponse(GetMany_Callback<T>, request);
        }

        private void GetMany_Callback<T>(IAsyncResult result) where T : Data.Index
        {
            var request = result.AsyncState as HttpWebRequest;

            var context = this.GetContext<T>(request);
            this.DeleteContext<T>(request);

            var response = request.EndGetResponse(result) as HttpWebResponse;
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream);

            var json = reader.ReadToEnd();
            var array = JArray.Parse(json);

            var entities = new List<object>();
            foreach (var jsonDocument in array)
            {
                entities.Add(this.IndexMapper.Map(jsonDocument.ToString()));
            }

            context.Post(entities);
        }

        private void Save_Request<T>(IAsyncResult result) where T : Data.Index
        {
            var request = result.AsyncState as HttpWebRequest;
            var writer = new StreamWriter(request.EndGetRequestStream(result));

            var context = this.GetContext<T>(request);

            var index = context.State as T;

            writer.Write(index.ToJson().ToString());
            writer.Close();

            request.BeginGetResponse(Save_Callback<T>, request);
        }

        private void Save_Callback<T>(IAsyncResult result) where T : Data.Index
        {
            var request = result.AsyncState as HttpWebRequest;

            var context = this.GetContext<T>(request);
            this.DeleteContext<T>(request);

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

                entity.Name = responseJson["Index"].ToString().Replace("\"", string.Empty);
                entity.Id = entity.Name;

                saveResponse.StatusCode = response.StatusCode;
            }
            catch (WebException ex)
            {
                var response = ex.Response as HttpWebResponse;
                if (response == null)
                {
                    throw;
                }

                var stream = response.GetResponseStream();
                var reader = new StreamReader(stream);
                var json = reader.ReadToEnd();

                JObject jResponse = JObject.Parse(json);

                saveResponse.StatusCode = response.StatusCode;
                saveResponse.Exception = new Exception(jResponse["Error"].ToString());
            }

            context.Post(saveResponse);
        }

        private void Delete_Callback<T>(IAsyncResult result) where T : Data.Index
        {
            var request = result.AsyncState as HttpWebRequest;

            var context = this.GetContext<T>(request);
            this.DeleteContext<T>(request);

            var response = request.EndGetResponse(result) as HttpWebResponse;
            var entity = context.State as T;

            var saveResponse = new SaveResponse();
            saveResponse.Entity = entity;
            saveResponse.StatusCode = response.StatusCode;

            context.Post(saveResponse);
        }
    }
}
