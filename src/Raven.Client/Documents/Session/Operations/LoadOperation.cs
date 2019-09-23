using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.TimeSeries;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Session.Operations
{
    internal class LoadOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LoadOperation>("Client");

        private string[] _ids ;
        private string[] _includes;
        private string[] _countersToInclude;
        private bool _includeAllCounters;
        private IEnumerable<TimeSeriesRange> _timeSeriesToInclude;

        private GetDocumentsResult _currentLoadResults;

        public LoadOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public GetDocumentsCommand CreateRequest()
        {
            if (_session.CheckIfIdAlreadyIncluded(_ids, _includes))
                return null;

            _session.IncrementRequestCount();
            if (Logger.IsInfoEnabled)
                Logger.Info($"Requesting the following ids '{string.Join(", ", _ids)}' from {_session.StoreIdentifier}");

            if (_includeAllCounters)
                return new GetDocumentsCommand(_ids, _includes, includeAllCounters: true, metadataOnly: false, timeSeriesIncludes : _timeSeriesToInclude);

            return _countersToInclude != null
                ? new GetDocumentsCommand(_ids, _includes, _countersToInclude, metadataOnly: false, timeSeriesIncludes: _timeSeriesToInclude)
                : new GetDocumentsCommand(_ids, _includes, metadataOnly: false, timeSeriesIncludes: _timeSeriesToInclude);
        }

        public LoadOperation ById(string id)
        {
            if (id == null)
                return this;

            if (_ids == null)
                _ids = new[] {id};

            return this;
        }

        public LoadOperation WithIncludes(string[] includes)
        {
            _includes = includes;
            return this;
        }

        public LoadOperation WithCounters(string[] counters)
        {
            if (counters != null)
                _countersToInclude = counters;
            return this;
        }

        public LoadOperation WithAllCounters()
        {
            _includeAllCounters = true;
            return this;
        }

        public LoadOperation WithTimeSeries(IEnumerable<TimeSeriesRange> timeseries)
        {
            if (timeseries != null)
                _timeSeriesToInclude = timeseries;
            return this;
        }

        public LoadOperation ByIds(IEnumerable<string> ids)
        {
            _ids = ids.Where(x => x != null).ToArray();

            foreach (var id in _ids.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                ById(id);
            }

            return this;
        }

        public T GetDocument<T>()
        {
            if (_session.NoTracking)
            {
                if (_currentLoadResults == null)
                    throw new InvalidOperationException($"Cannot execute '{nameof(GetDocument)}' before operation execution.");

                var document = _currentLoadResults.Results[0] as BlittableJsonReaderObject;
                if (document == null)
                    return default;

                var documentInfo = DocumentInfo.GetNewDocumentInfo(document);

                return _session.TrackEntity<T>(documentInfo);
            }

            return GetDocument<T>(_ids[0]);
        }

        private T GetDocument<T>(string id)
        {
            if (id == null)
                return default;

            if (_session.IsDeleted(id))
                return default;

            if (_session.DocumentsById.TryGetValue(id, out var doc))
                return _session.TrackEntity<T>(doc);

            if (_session.IncludedDocumentsById.TryGetValue(id, out doc))
                return _session.TrackEntity<T>(doc);

            return default;
        }

        public Dictionary<string, T> GetDocuments<T>()
        {
            var finalResults = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            if (_session.NoTracking)
            {
                if (_currentLoadResults == null)
                    throw new InvalidOperationException($"Cannot execute '{nameof(GetDocuments)}' before operation execution.");

                foreach (var id in _ids)
                {
                    if (id == null)
                        continue;

                    finalResults[id] = default;
                }

                foreach (var document in GetDocumentsFromResult(_currentLoadResults))
                    finalResults[document.Id] = _session.TrackEntity<T>(document);

                return finalResults;
            }

            foreach (var id in _ids)
            {
                if (id == null)
                    continue;
                finalResults[id] = GetDocument<T>(id);
            }
            return finalResults;
        }

        public void SetResult(GetDocumentsResult result)
        {
            if (result == null)
                return;

            if (_session.NoTracking)
            {
                _currentLoadResults = result;
                return;
            }

            _session.RegisterIncludes(result.Includes);

            if (_includeAllCounters || _countersToInclude != null)
            {
                _session.RegisterCounters(result.CounterIncludes, _ids.ToArray(), _countersToInclude, _includeAllCounters);
            }

            if (_timeSeriesToInclude != null)
            {
                _session.RegisterTimeSeries(result.TimeSeriesIncludes);
            }

            foreach (var document in GetDocumentsFromResult(result))
                _session.DocumentsById.Add(document);

            _session.RegisterMissingIncludes(result.Results, result.Includes, _includes);

        }

        private static IEnumerable<DocumentInfo> GetDocumentsFromResult(GetDocumentsResult result)
        {
            foreach (BlittableJsonReaderObject document in result.Results)
            {
                if (document == null)
                    continue;

                yield return DocumentInfo.GetNewDocumentInfo(document);
            }
        }
    }
}
