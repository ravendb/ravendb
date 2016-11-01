using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Documents.Commands;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.SessionOperations
{
    public class LoadTransformerOperation
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.Client");
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
                TransformerParameters = _transformerParameters
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

        public T[] GetTransformedDocuments<T>(GetDocumentResult result)
        {
            if (typeof(T).IsArray)
            {
                var arrayOfArrays = result.Results
                    .Select(x =>
                    {
                        if (x == null)
                            return null;

                        BlittableJsonReaderArray values;
                        if (((BlittableJsonReaderObject) x).TryGet("$values", out values) == false)
                            throw new InvalidOperationException("Transformed document must have a $values property");

                        var elementType = typeof(T).GetElementType();
                        var array = values.Select(value => _session.EntityToBlittable.ConvertToEntity(elementType, null, value as BlittableJsonReaderObject)).ToArray();
                        var newArray = Array.CreateInstance(elementType, array.Length);
                        Array.Copy(array, newArray, array.Length);
                        return newArray;
                    })
                    .Cast<T>()
                    .ToArray();

                return arrayOfArrays;
            }

            var items = ParseResults<T>(result.Results).ToArray();

            if (items.Length > _ids.Length)
            {
                throw new InvalidOperationException(String.Format("A load was attempted with transformer {0}, and more than one item was returned per entity - please use {1}[] as the projection type instead of {1}",
                    _transformer,
                    typeof(T).Name));
            }

            return items;
        }

        public void SetResult(GetDocumentResult result)
        {
            if (result.Includes != null)
            {
                foreach (BlittableJsonReaderObject include in result.Includes)
                {
                    var newDocumentInfo = InMemoryDocumentSessionOperations.DocumentInfo.GetNewDocumentInfo(include);
                    _session.includedDocumentsByKey[newDocumentInfo.Id] = newDocumentInfo;
                }
            }

            if (_includes != null && _includes.Length > 0)
            {
                _session.RegisterMissingIncludes(result.Results, _includes);
            }
        }
        
        private IEnumerable<T> ParseResults<T>(BlittableJsonReaderArray results)
        {
            foreach (BlittableJsonReaderObject result in results)
            {
                if (result == null)
                {
                    yield return default(T);
                    continue;
                }

                //EnsureNotReadVetoed(result); Do we need this?

                BlittableJsonReaderArray values;
                if (result.TryGet("$values", out values) == false)
                    throw new InvalidOperationException("Transformed document must have a $values property");

                foreach (BlittableJsonReaderObject value in values)
                {
                    yield return (T)_session.EntityToBlittable.ConvertToEntity(typeof(T), null, value);
                }
            }
        }

        /*private bool EnsureNotReadVetoed(BlittableJsonReaderObject result)
        {
            BlittableJsonReaderObject metadata;
            if (result.TryGet(Constants.Metadata.Key, out metadata) == false)
                throw new InvalidOperationException("Document must have a metadata");

            if (metadata != null)
                _session.EnsureNotReadVetoed(metadata); // this will throw on read veto

            return true;
        }*/
    }
}
