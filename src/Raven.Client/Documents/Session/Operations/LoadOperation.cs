using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Session.Operations
{
    internal class LoadOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.NewClient.Client");

        private string[] _ids;
        private string[] _includes;
        private readonly List<string> _idsToCheckOnServer = new List<string>();
        public LoadOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public GetDocumentCommand CreateRequest()
        {
            if (_idsToCheckOnServer.Count == 0)
                return null;

            if (_session.CheckIfIdAlreadyIncluded(_ids, _includes))
                return null;

            _session.IncrementRequestCount();
            if (Logger.IsInfoEnabled)
                Logger.Info($"Requesting the following ids '{string.Join(", ", _idsToCheckOnServer)}' from {_session.StoreIdentifier}");
            return new GetDocumentCommand(_idsToCheckOnServer.ToArray(), _includes, transformer: null, transformerParameters: null, metadataOnly: false, context: _session.Context);
        }

        public LoadOperation ById(string id)
        {
            if (id == null)
                return this;

            if (_ids == null)
                _ids = new[] { id };

            if (_session.IsLoadedOrDeleted(id))
                return this;

            _idsToCheckOnServer.Add(id);
            return this;
        }

        public LoadOperation WithIncludes(string[] includes)
        {
            _includes = includes;
            return this;
        }

        public LoadOperation ByIds(IEnumerable<string> ids)
        {
            _ids = ids.ToArray();
            foreach (var id in _ids.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                ById(id);
            }

            return this;
        }

        public T GetDocument<T>()
        {
            return GetDocument<T>(_ids[0]);
        }

        private T GetDocument<T>(string id)
        {
            if (id == null)
                return default(T);

            if (_session.IsDeleted(id))
                return default(T);

            DocumentInfo doc;
            if (_session.DocumentsById.TryGetValue(id, out doc))
                return _session.TrackEntity<T>(doc);

            if (_session.IncludedDocumentsByKey.TryGetValue(id, out doc))
                return _session.TrackEntity<T>(doc);

            return default(T);
        }

        public Dictionary<string, T> GetDocuments<T>()
        {
            var finalResults = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < _ids.Length; i++)
            {
                var id = _ids[i];
                if (id == null)
                    continue;
                finalResults[id] = GetDocument<T>(id);
            }
            return finalResults;
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

            foreach (BlittableJsonReaderObject document in result.Results)
            {
                if (document == null)
                    continue;

                var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(document);
                _session.DocumentsById.Add(newDocumentInfo);
            }
            
            if (_includes != null && _includes.Length > 0)
            {
                _session.RegisterMissingIncludes(result.Results, _includes);
            }
        }
    }
}