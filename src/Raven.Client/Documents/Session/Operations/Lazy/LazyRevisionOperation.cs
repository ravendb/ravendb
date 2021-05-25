using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyRevisionOperation<T> : ILazyOperation
    {
        private readonly GetRevisionOperation _getRevisionOperation;
        private Mode _mode;

        public enum Mode
        {
            Single,
            Multi,
            Map,
            ListOfMetadata
        }
        
        public LazyRevisionOperation(GetRevisionOperation getRevisionOperation, Mode mode)
        {
            _getRevisionOperation = getRevisionOperation;
            _mode = mode;
        }

        public object Result { get; set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; set; }
        
        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            GetRevisionsCommand getRevisionsCommand = _getRevisionOperation.Command;
            var sb = new StringBuilder("?");
            getRevisionsCommand.GetRequestQueryString(sb);
            return new GetRequest {Method = HttpMethod.Get, Url = "/revisions", Query = sb.ToString()};
        }

        public void HandleResponse(GetResponse response)
        {
            BlittableJsonReaderObject responseAsBlittableReaderObject = (BlittableJsonReaderObject)response.Result;
            responseAsBlittableReaderObject.TryGet("Results", out BlittableJsonReaderArray blittableJsonReaderArray);

            _getRevisionOperation.SetResult(new BlittableArrayResult {Results = blittableJsonReaderArray});
            switch (_mode)
            {
                case Mode.Single:
                    Result = _getRevisionOperation.GetRevision<T>();
                    break;
                case Mode.Multi:
                    Result = _getRevisionOperation.GetRevisionsFor<T>();
                    break;
                case Mode.Map:
                    Result = _getRevisionOperation.GetRevisions<T>();
                    break;
                case Mode.ListOfMetadata:
                    Result = _getRevisionOperation.GetRevisionsMetadataFor();
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
