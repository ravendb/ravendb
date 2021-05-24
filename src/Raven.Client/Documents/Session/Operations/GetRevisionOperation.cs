using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Commands;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Session.Operations
{
    internal class GetRevisionOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;

        private BlittableArrayResult _result;
        private readonly GetRevisionsCommand _command;

        public GetRevisionOperation(InMemoryDocumentSessionOperations session, string id, int? start, int? pageSize, bool metadataOnly = false)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _command = new GetRevisionsCommand(id, start, pageSize, metadataOnly);
        }

        public GetRevisionOperation(InMemoryDocumentSessionOperations session, string id, DateTime before)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _command = new GetRevisionsCommand(id, before);
        }

        public GetRevisionOperation(InMemoryDocumentSessionOperations session, string changeVector)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _command = new GetRevisionsCommand(changeVector);
        }

        public GetRevisionOperation(InMemoryDocumentSessionOperations session, IEnumerable<string> changeVectors)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _command = new GetRevisionsCommand(changeVectors.ToArray());
        }

        public GetRevisionsCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            return _command;
        }

        public void SetResult(BlittableArrayResult result)
        {
            _result = result;
        }

        private T GetRevision<T>(BlittableJsonReaderObject document)
        {
            if (document == null)
                return default;

            var metadata = document.GetMetadata();
            var id = metadata.GetId();
            var entity = _session.JsonConverter.FromBlittable<T>(ref document, id, _session.NoTracking == false);

            _session.DocumentsByEntity[entity] = new DocumentInfo
            {
                Id = id,
                ChangeVector = metadata.GetChangeVector(),
                Document = document,
                Metadata = metadata,
                Entity = entity
            };

            return entity;
        }

        public List<T> GetRevisionsFor<T>()
        {
            var results = new List<T>(_result.Results.Length);
            foreach (BlittableJsonReaderObject revision in _result.Results)
            {
                results.Add(GetRevision<T>(revision));
            }
            return results;
        }

        public List<MetadataAsDictionary> GetRevisionsMetadataFor()
        {
            var results = new List<MetadataAsDictionary>(_result.Results.Length);
            foreach (BlittableJsonReaderObject revision in _result.Results)
            {
                var metadata = revision.GetMetadata();
                results.Add(new MetadataAsDictionary(metadata));
            }
            return results;
        }

        public T GetRevision<T>()
        {
            if (_result == null)
                return default(T);

            var document = (BlittableJsonReaderObject)_result.Results[0];
            return GetRevision<T>(document);
        }

        public Dictionary<string, T> GetRevisions<T>()
        {
            var results = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < _command.ChangeVectors.Length; i++)
            {
                var changeVector = _command.ChangeVectors[i];
                if (changeVector == null)
                    continue;
                results[changeVector] = GetRevision<T>((BlittableJsonReaderObject)_result.Results[i]);
            }
            return results;
        }
    }

    internal class GetRevisionsCountOperation
    {
        private readonly string _docId;

        public GetRevisionsCountOperation(string docId)
        {
            _docId = docId;
        }

        public RavenCommand<long> CreateRequest()
        {
            return new GetRevisionsCountCommand(_docId);
        }

        private class GetRevisionsCountCommand : RavenCommand<long>
        {
            private readonly string _id;

            public GetRevisionsCountCommand(string id)
            {
                _id = id ?? throw new ArgumentNullException(nameof(id));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/revisions/count?")
                    .Append("&id=")
                    .Append(Uri.EscapeDataString(_id));

                url = pathBuilder.ToString();
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                {
                    Result = default;
                    return;
                }

                Result = JsonDeserializationClient.DocumentRevisionsCount(response).RevisionsCount;
            }

            public override bool IsReadRequest => true;
        }

        internal class DocumentRevisionsCount : IDynamicJson
        {
            public long RevisionsCount { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(RevisionsCount)] = RevisionsCount
                };
            }
        }
    }
}
