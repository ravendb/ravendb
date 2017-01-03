using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Json.Utilities;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class LoadTransformerOperation
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.NewClient.Client");
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
            if (_logger.IsInfoEnabled)
                _logger.Info($"Requesting the following ids '{string.Join(", ", _idsToCheckOnServer)}' from {_session.StoreIdentifier}");

            return new GetDocumentCommand
            {
                Ids = _idsToCheckOnServer.ToArray(),
                Includes = _includes,
                Transformer = _transformer,
                TransformerParameters = _transformerParameters,
                Context = _session.Context
            };
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
        
        public IDictionary<string, T> GetTransformedDocuments<T>(GetDocumentResult result, string[] ids)
        {
            if (result == null || ids == null)
                return null;

            var parsedResutlsDictionary = new Dictionary<string, T>();
            T[] parsedResults;

            if (typeof(T).IsArray)
            {
                parsedResults = TransformerHelpers.ParseResultsArray<T>(_session, result);
            }
            else
            {
                parsedResults = TransformerHelpers.ParseResults<T>(_session, result).ToArray();

                if (parsedResults.Length > _ids.Length)
                {
                    throw new InvalidOperationException(
                        $"A load was attempted with transformer {_transformer}, and more than one item was returned per entity - please use {typeof(T).Name}[] as the projection type instead of {typeof(T).Name}");
                }
            }

            var i = 0;
            foreach (var id in ids)
            {
                if (id == null)
                    continue;

                parsedResutlsDictionary.Add(id, parsedResults[i]);
                i++;
            }
            return parsedResutlsDictionary;
        }

        public void SetResult(GetDocumentResult result)
        {
            if (result.Includes != null)
            {
                foreach (BlittableJsonReaderObject include in result.Includes)
                {
                    var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(include);
                    _session.includedDocumentsByKey[newDocumentInfo.Id] = newDocumentInfo;
                }
            }

            if (_includes != null && _includes.Length > 0)
            {
                _session.RegisterMissingIncludes(result.Results, _includes);
            }
        }
    }
}
