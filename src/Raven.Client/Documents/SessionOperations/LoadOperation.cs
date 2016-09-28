using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Sparrow.Logging;
using DocumentInfo = Raven.Client.Documents.InMemoryDocumentSessionOperations.DocumentInfo;
namespace Raven.Client.Documents.SessionOperations
{
    public class LoadOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.Client");

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
            if (_logger.IsInfoEnabled)
                _logger.Info($"Requesting the following ids '{string.Join(", ", _idsToCheckOnServer)}' from {_session.StoreIdentifier}");
            return new GetDocumentCommand
            {
                Ids = _idsToCheckOnServer.ToArray(),
                Includes = _includes
            };
        }

        public void ById(string id)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id), "The document id cannot be null");

            if (_ids == null)
                _ids = new[] {id};
            if (_session.IsLoadedOrDeleted(id))
                return;

            _idsToCheckOnServer.Add(id);
        }

        public void WithIncludes(string[] includes)
        {
            this._includes = includes;
        }

        public void ByIds(IEnumerable<string> ids)
        {
            _ids = ids.ToArray();
            foreach (var id in _ids.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                ById(id);
            }
        }

        public T GetDocument<T>()
        {
            return GetDocument<T>(_ids[0]);
        }

        private T GetDocument<T>(string id)
        {
            if (_session.IsDeleted(id))
                return default(T);

            DocumentInfo documentInfo;
            if (_session.DocumentsById.TryGetValue(id, out documentInfo) == false)
            {
                if (_session.includedDocumentsByKey.TryGetValue(id, out documentInfo))
                {
                    _session.includedDocumentsByKey.Remove(id);
                    _session.DocumentsById[id] = documentInfo;
                }
                else
                {
                    return default(T);
                }
            }

            if (documentInfo.Entity != null)
                return (T)documentInfo.Entity;

            if (documentInfo.Document == null)
                return default(T);

            var entity = _session.ConvertToEntity(typeof(T), id, documentInfo.Document);
            documentInfo.Entity = entity;
            try
            {
                _session.DocumentsByEntity.Add(entity, documentInfo);
            }
            catch (Exception)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Tried to add an exisitg entity");
            }
            
            return (T) entity;
        }

        public T[] GetDocuments<T>()
        {
            var finalResults = new T[_ids.Length];
            for (int i = 0; i < _ids.Length; i++)
            {
                var id = _ids[i];
                finalResults[i] = GetDocument<T>(id);
            }
            return finalResults;
        }

        public void SetResult(GetDocumentResult result)
        {
            if (result.Includes != null && result.Includes.Any())
            {
                foreach (BlittableJsonReaderObject document in result.Includes)
                {
                    if (document == null)
                    {
                        // Not sure this is possible without changing the result class
                        // TODO: _session.RegisterMissing(includeIds[i]);
                        continue;
                    }

                    BlittableJsonReaderObject metadata;
                    if (document.TryGet(Constants.Metadata.Key, out metadata) == false)
                        throw new InvalidOperationException("Document must have a metadata");
                    string id;
                    if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                        throw new InvalidOperationException("Document must have an id");
                    long etag;
                    if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                        throw new InvalidOperationException("Document must have an etag");
                    var newMetadata = new DocumentInfo
                    {
                        //TODO - Add all DocumentInfo properties ??
                        Id = id,
                        Document = document,
                        Metadata = metadata,
                        ETag = etag
                    };

                    _session.includedDocumentsByKey[id] = newMetadata;
                }
            }

            for (var i = 0; i < result.Results.Length; i++)
            {
                var document = (BlittableJsonReaderObject)result.Results[i];
                if (document == null)
                {
                    _session.RegisterMissing(_idsToCheckOnServer[i]);
                    continue;
                }

                BlittableJsonReaderObject metadata;
                if (document.TryGet(Constants.Metadata.Key, out metadata) == false)
                    throw new InvalidOperationException("Document must have a metadata");
                string id;
                if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                    throw new InvalidOperationException("Document must have an id");
                long etag;
                if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                    throw new InvalidOperationException("Document must have an etag");
                var newMetadata = new DocumentInfo
                {
                    //TODO - Add all DocumentInfo properties ??
                    Id = id,
                    Document = document,
                    Metadata = metadata,
                    ETag = etag
                };

                _session.DocumentsById[id] = newMetadata;
            }

            if (_includes != null && _includes.Length > 0)
            {
                _session.RegisterMissingIncludes(result.Results, _includes);
            }
        }
    }
}