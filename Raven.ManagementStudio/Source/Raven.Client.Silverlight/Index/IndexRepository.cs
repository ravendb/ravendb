namespace Raven.Client.Silverlight.Index
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

    public class IndexRepository : BaseRepository<Data.Index>, IIndexRepository
    {
        public IndexRepository(Uri databaseAddress)
        {
            WebRequest.RegisterPrefix("http://", WebRequestCreator.ClientHttp);

            this.ContextStorage = new Dictionary<HttpWebRequest, SynchronizationContext>();
            this.DatabaseAddress = databaseAddress;

            this.Mapper = new IndexMapper();
        }

        public void Get<T>(string name, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : Data.Index
        {
            throw new NotImplementedException();
        }

        public void GetMany<T>(string[] names, IList<T> existingEntities, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : Data.Index
        {
            HttpWebRequest request;

            if (names == null || names.Length == 0)
            {
                this.CreateContext(new Uri(string.Format(DatabaseUrl.IndexGetAll, this.DatabaseAddress)), RequestMethod.GET, out request);

                request.BeginGetResponse((result) => this.GetMany_Callback(existingEntities, request, result, callback, storeCallback), request);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void Put<T>(T entity, CallbackFunction.Save<T> callback, CallbackFunction.Store<T> storeCallback) where T : Data.Index
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.IndexPut, this.DatabaseAddress, entity.Name)), RequestMethod.PUT, out request);

            request.BeginGetRequestStream((requestResult) =>
                                              {
                                                  var writer = new StreamWriter(request.EndGetRequestStream(requestResult));

                                                  writer.Write(entity.ToJson());
                                                  writer.Close();

                                                  request.BeginGetResponse((responseResult) => this.Save_Callback(entity, request, responseResult, callback, storeCallback), request);
                                              },
                                              request);
        }

        public void Delete<T>(T entity, CallbackFunction.Save<T> callback) where T : Data.Index
        {
            HttpWebRequest request;
            this.CreateContext(new Uri(string.Format(DatabaseUrl.IndexPut, this.DatabaseAddress, entity.Name)), RequestMethod.DELETE, out request);

            request.BeginGetResponse((result) => this.Delete_Callback(entity, request, result, callback), request);
        }

        private void GetMany_Callback<T>(IList<T> existingEntities, HttpWebRequest request, IAsyncResult result, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : Data.Index
        {
            var context = this.GetContext(request);

            HttpStatusCode statusCode;
            var json = this.GetResponseStream(request, result, out statusCode);
            var array = JArray.Parse(json);

            var entities = array.Select(jsonIndex => (T)this.Mapper.Map(jsonIndex.ToString()));
            var responseResult = existingEntities != null ? entities.Concat(existingEntities).ToList() : entities.ToList();

            var loadResponse = new LoadResponse<IList<T>>()
                                   {
                                       Data = responseResult,
                                       StatusCode = statusCode
                                   };

            context.Post(delegate
                             {
                                 callback.Invoke(loadResponse);
                             },
                         null);

            context.Post(delegate
                             {
                                 storeCallback.Invoke(loadResponse.Data);
                             },
                         null);
        }

        private void Save_Callback<T>(T entity, HttpWebRequest request, IAsyncResult result, CallbackFunction.Save<T> callback, CallbackFunction.Store<T> storeCallback) where T : Data.Index
        {
            var context = this.GetContext(request);

            HttpStatusCode statusCode;
            var json = this.GetResponseStream(request, result, out statusCode);

            var responseJson = JObject.Parse(json);

            entity.Name = entity.Id = responseJson["Index"].ToString().Replace("\"", string.Empty);

            var saveResponse = new SaveResponse<T>()
                                   {
                                       Data = entity,
                                       StatusCode = statusCode
                                   };

            context.Post(delegate
                             {
                                 callback.Invoke(saveResponse);
                             },
                         null);

            context.Post(delegate
                             {
                                 storeCallback.Invoke(saveResponse.Data);
                             },
                         null);
        }

        private void Delete_Callback<T>(T entity, HttpWebRequest request, IAsyncResult result, CallbackFunction.Save<T> callback) where T : Data.Index
        {
            var context = this.GetContext(request);

            HttpStatusCode statusCode;
            var json = this.GetResponseStream(request, result, out statusCode);

            var deleteResponse = new DeleteResponse<T>()
                                     {
                                         Data = entity,
                                         StatusCode = statusCode
                                     };

            context.Post(delegate
                             {
                                 callback.Invoke(deleteResponse);
                             },
                         null);
        }
    }
}
