using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;


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
                    var res = result.Current;
                    query.InvokeAfterStreamExecuted(res);
                    var stremResult = CreateStreamResult<T>(ref res);
                    yield return stremResult;
                }
            }
        }

        private StreamResult<T> CreateStreamResult<T>(ref BlittableJsonReaderObject res)
        {
            string key = null;
            long? etag = null;
            BlittableJsonReaderObject metadata;
            if (res.TryGet(Constants.Metadata.Key, out metadata))
            {
                if (metadata.TryGet(Constants.Metadata.Id, out key) == false)
                    throw new ArgumentException();
                if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                    throw new ArgumentException();
            }
            //TODO - Investagate why ConvertToEntity fails if we don't call ReadObject before
            res = Context.ReadObject(res, key);
            var entity = ConvertToEntity(typeof(T), key, res);
            var stremResult = new StreamResult<T>
            {
                Etag = etag.Value,
                Key = key,
                Document = (T) entity,
                Metadata = new MetadataAsDictionary(metadata)
            };
            return stremResult;
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
                    var res = result.Current;
                    var stremResult = CreateStreamResult<T>(ref res);
                    yield return stremResult;
                }
            }
        }
    }

}