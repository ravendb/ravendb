using System;
using System.Collections.Generic;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Json.Utilities;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class LoadStartingWithOperation
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadStartingWithOperation>("Raven.NewClient.Client");
        private readonly InMemoryDocumentSessionOperations _session;

        private string _startWith;
        private string _matches;
        private int _start;
        private int _pageSize;
        private string _exclude;
        private RavenPagingInformation _pagingInformation;
        private string _skipAfter;

        private string _transformer;
        private Dictionary<string, object> _transformerParameters;

        private readonly List<string> _returnedIds = new List<string>();

        public LoadStartingWithOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public GetDocumentCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            if (_logger.IsInfoEnabled)
                _logger.Info($"Requesting documents with ids starting with '{_startWith}' from {_session.StoreIdentifier}");

            return new GetDocumentCommand
            {
                StartWith = _startWith,
                Matches = _matches,
                Start = _start,
                PageSize = _pageSize,
                Exclude = _exclude,
                PagingInformation = _pagingInformation,
                SkipAfter = _skipAfter,

                Transformer = _transformer,
                TransformerParameters = _transformerParameters
            };
        }

        public void WithStartWith(string keyPrefix, string matches = null, int start = 0, int pageSize = 25,
            string exclude = null, RavenPagingInformation pagingInformation = null,
            Action<ILoadConfiguration> configure = null,
            string skipAfter = null)
        {
            _startWith = keyPrefix;
            _matches = matches;
            _start = start;
            _pageSize = pageSize;
            _exclude = exclude;
            _pagingInformation = pagingInformation;
            _skipAfter = skipAfter;
        }

        public void WithTransformer(string transformer, Dictionary<string, object> transformerParameters)
        {
            _transformer = transformer;
            _transformerParameters = transformerParameters;
        }

        public void SetResult(GetDocumentResult result)
        {
            // We don't want to track transformed entities.
            if (_transformer != null)
                return;

            _pagingInformation?.Fill(_start, _pageSize, result.NextPageStart);

            foreach (BlittableJsonReaderObject document in result.Results)
            {
                var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(document);
                _session.DocumentsById.Add(newDocumentInfo);
                _returnedIds.Add(newDocumentInfo.Id);
            }
        }

        public T[] GetDocuments<T>()
        {
            var i = 0;
            var finalResults = new T[_returnedIds.Count];
            foreach (var id in _returnedIds)
            {
                finalResults[i++] = GetDocument<T>(id);
            }
            return finalResults;
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

            return default(T);
        }

        public Dictionary<string, T> GetTransformedDocuments<T>(GetDocumentResult result)
        {
            if (result == null)
                return null;

            return TransformerHelper.ParseResultsForLoadOperation<T>(_session, result);
        }
    }
}
