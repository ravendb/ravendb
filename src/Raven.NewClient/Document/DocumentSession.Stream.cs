using System;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Extensions;
using Raven.NewClient.Json.Utilities;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document
{
    public partial class DocumentSession
    {
        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query)
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return Stream(docQuery);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query)
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest((IRavenQueryInspector)query);
            RequestExecuter.Execute(command, Context);
            using (var result = streamOperation.SetResult(command.Result))
            {
                while (result.MoveNext())
                {
                    var json = result.Current;
                    query.InvokeAfterStreamExecuted(json);

                    if (command.UsedTransformer)
                    {
                        foreach (var streamResult in CreateMultipleStreamResults<T>(json))
                            yield return streamResult;

                        continue;
                    }

                    yield return CreateStreamResult<T>(ref json);
                }
            }
        }

        private IEnumerable<StreamResult<T>> CreateMultipleStreamResults<T>(BlittableJsonReaderObject json)
        {
            BlittableJsonReaderArray values;
            if (json.TryGet(Constants.Json.Fields.Values, out values) == false)
                throw new InvalidOperationException("Transformed document must have a $values property");

            var metadata = json.GetMetadata();
            var etag = metadata.GetEtag();
            var id = metadata.GetId();

            foreach (var value in TransformerHelpers.ParseValuesFromBlittableArray<T>(this, values))
            {
                yield return new StreamResult<T>
                {
                    Key = id,
                    Etag = etag,
                    Document = value,
                    Metadata = new MetadataAsDictionary(metadata)
                };
            }
        }

        private StreamResult<T> CreateStreamResult<T>(ref BlittableJsonReaderObject json)
        {
            var metadata = json.GetMetadata();
            var etag = metadata.GetEtag();
            var id = metadata.GetId();

            //TODO - Investagate why ConvertToEntity fails if we don't call ReadObject before
            json = Context.ReadObject(json, id);
            var entity = ConvertToEntity(typeof(T), id, json);
            var streamResult = new StreamResult<T>
            {
                Etag = etag,
                Key = id,
                Document = (T)entity,
                Metadata = new MetadataAsDictionary(metadata)
            };
            return streamResult;
        }

        public IEnumerator<StreamResult<T>> Stream<T>(long? fromEtag, int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string transformer = null,
            Dictionary<string, object> transformerParameters = null)
        {
            return Stream<T>(fromEtag: fromEtag, startsWith: null, matches: null, start: start, pageSize: pageSize, pagingInformation: pagingInformation,
                skipAfter: null, transformer: transformer, transformerParameters: transformerParameters);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null,
            Dictionary<string, object> transformerParameters = null)
        {
            return Stream<T>(fromEtag: null, startsWith: startsWith, matches: matches, start: start, pageSize: pageSize, pagingInformation: pagingInformation,
                skipAfter: skipAfter, transformer: transformer, transformerParameters: transformerParameters);
        }

        private IEnumerator<StreamResult<T>> Stream<T>(long? fromEtag, string startsWith, string matches, int start, int pageSize, RavenPagingInformation pagingInformation,
            string skipAfter, string transformer, Dictionary<string, object> transformerParameters)
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(fromEtag, startsWith, matches, start, pageSize, null, pagingInformation, skipAfter, transformer,
                transformerParameters);
            RequestExecuter.Execute(command, Context);
            using (var result = streamOperation.SetResult(command.Result))
            {
                while (result.MoveNext())
                {
                    var json = result.Current;

                    if (command.UsedTransformer)
                    {
                        foreach (var streamResult in CreateMultipleStreamResults<T>(json))
                            yield return streamResult;

                        continue;
                    }

                    yield return CreateStreamResult<T>(ref json);
                }
            }
        }
    }

}