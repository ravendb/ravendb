using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Document.SessionOperations
{
    public class NewLoadOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggerSetup.Instance.GetLogger<NewLoadOperation>("Raven.Client");

        private SortedSet<string> _ids;
        private readonly List<string> _idsToCheckOnServer = new List<string>();

        public NewLoadOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public GetDocumentCommand CreateRequest()
        {
            if (_idsToCheckOnServer.Count == 0)
                return null;

            _session.IncrementRequestCount();
            if (_logger.IsInfoEnabled)
                _logger.Info($"Requesting the following ids '{string.Join(", ", _idsToCheckOnServer)}' from {_session.StoreIdentifier}");
            return new GetDocumentCommand
            {
                Ids = _idsToCheckOnServer.ToArray()
            };
        }

        public void ById(string id)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id), "The document id cannot be null");

            if (_ids == null)
                _ids = new SortedSet<string> {id};
            if (_session.IsLoadedOrDeleted(id))
                return;

            _idsToCheckOnServer.Add(id);
        }

        public void ByIds(IEnumerable<string> ids)
        {
            _ids = new SortedSet<string>(ids);
            foreach (var id in _ids)
            {
                ById(id);
            }
        }

        public T GetDocument<T>()
        {
            return GetDocument<T>(_ids.ElementAt(0));
        }

        private T GetDocument<T>(string id)
        {
            if (_session.IsDeleted(id))
                return default(T);

            object existingEntity;
            if (_session.EntitiesById.TryGetValue(id, out existingEntity))
                return (T) existingEntity;

            BlittableJsonReaderObject document;
            if (_session.DocumentsById.TryGetValue(id, out document) == false)
                return default(T);

            var entity = _session.ConvertToEntity(typeof(T), id, document);
            try
            {
                _session.IdByEntities.Add(entity, id);
            }
            catch (Exception)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Tried to add an exisitg entity");
            }

            try
            {
                _session.EntitiesById.Add(id, entity);
            }
            catch (Exception)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Tried to add an exisitg id");
            }
            return (T) entity;
        }

        public T[] GetDocuments<T>()
        {
            var finalResults = new T[_ids.Count];
            for (int i = 0; i < _ids.Count; i++)
            {
                var id = _ids.ElementAt(i);
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
                        // TODO: _session.RegisterMissing(includeIds[i]);
                        continue;
                    }

                    BlittableJsonReaderObject metadata;
                    if (document.TryGet(Constants.Metadata, out metadata) == false)
                        throw new InvalidOperationException("Document must have a metadata");
                    string id;
                    if (metadata.TryGet(Constants.MetadataDocId, out id) == false)
                        throw new InvalidOperationException("Document must have an id");
                    _session.DocumentsById[id] = document;
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
                if (document.TryGet(Constants.Metadata, out metadata) == false)
                    throw new InvalidOperationException("Document must have a metadata");
                string id;
                if (metadata.TryGet(Constants.MetadataDocId, out id) == false)
                    throw new InvalidOperationException("Document must have an id");
                _session.DocumentsById[id] = document;
            }
        }
    }
}