using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Transformers;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Session.Operations
{
    internal class LoadTransformerOperation
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.NewClient.Client");
        private readonly InMemoryDocumentSessionOperations _session;

        private string[] _ids;
        private string[] _includes;
        private string _transformer;
        private Dictionary<string, object> _transformerParameters;
        private readonly List<string> _idsToCheckOnServer = new List<string>();

        public LoadTransformerOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public GetDocumentCommand CreateRequest()
        {
            if (_idsToCheckOnServer.Count == 0)
                return null;

            _session.IncrementRequestCount();
            if (Logger.IsInfoEnabled)
                Logger.Info($"Requesting the following ids '{string.Join(", ", _idsToCheckOnServer)}' from {_session.StoreIdentifier}");

            return new GetDocumentCommand(_idsToCheckOnServer.ToArray(), _includes, _transformer, _transformerParameters, metadataOnly: false, context: _session.Context);
        }

        public void WithIncludes(string[] includes)
        {
            _includes = includes;
        }

        public void ById(string id)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id), "The document id cannot be null");

            if (_ids == null)
                _ids = new[] { id };

            _idsToCheckOnServer.Add(id);
        }

        public void ByIds(IEnumerable<string> ids)
        {
            _ids = ids.ToArray();
            foreach (var id in _ids.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                ById(id);
            }
        }

        public void WithTransformer(string transformer, Dictionary<string, object> transformerParameters)
        {
            _transformer = transformer;
            _transformerParameters = transformerParameters;
        }

        public Dictionary<string, T> GetTransformedDocuments<T>(GetDocumentResult result)
        {
            if (result == null)
            {
                var empty = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
                foreach (var id in _idsToCheckOnServer)
                    empty[id] = default(T);

                return empty;
            }

            return TransformerHelper.ParseResultsForLoadOperation<T>(_session, result, _idsToCheckOnServer);
        }

        public void SetResult(GetDocumentResult result)
        {
            if (result == null)
                return;

            if (result.Includes != null)
            {
                foreach (BlittableJsonReaderObject include in result.Includes)
                {
                    if (include == null)
                        continue;

                    var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(include);
                    _session.IncludedDocumentsByKey[newDocumentInfo.Id] = newDocumentInfo;
                }
            }

            if (_includes != null && _includes.Length > 0)
            {
                _session.RegisterMissingIncludes(result.Results, _includes);
            }
        }
    }
}
