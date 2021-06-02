using System;
using System.Net;
using System.Net.Http;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyConditionalLoadOperation<T> : ILazyOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly string _id;
        private readonly string _changeVector;

        public LazyConditionalLoadOperation(string id, string changeVector, InMemoryDocumentSessionOperations session)
        {
            _id = id;
            _changeVector = changeVector;
            _session = session;
        }

        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            var request = new GetRequest
            {
                Url = "/docs", 
                Method = HttpMethod.Get, 
                Query = $"?id={Uri.EscapeDataString(_id)}"
            };

            request.Headers.Add("If-None-Match", '"' + _changeVector + '"');
            return request;
        }

        public object Result { get; set; }
        public QueryResult QueryResult => throw new NotImplementedException();
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(GetResponse response)
        {
            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            switch (response.StatusCode)
            {
                case HttpStatusCode.NotModified:
                    Result = (default(T), _changeVector); // value not changed
                    return;
                case HttpStatusCode.NotFound:
                    _session.RegisterMissing(_id);
                    Result = default((T, string));
                    return;
            }
            
            if (response.Result != null)
            {
                var etag = response.Headers[Constants.Headers.Etag];
                var res = JsonDeserializationClient.ConditionalGetResult((BlittableJsonReaderObject)response.Result);
                var documentInfo = DocumentInfo.GetNewDocumentInfo((BlittableJsonReaderObject)res.Results[0]);
                var r = _session.TrackEntity<T>(documentInfo);

               Result = (r, etag);
               return;
            }

            Result = null;
            _session.RegisterMissing(_id);
        }
    }
}
