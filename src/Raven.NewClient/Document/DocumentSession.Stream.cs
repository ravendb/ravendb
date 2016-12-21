using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Linq;
using Sparrow.Json;


namespace Raven.NewClient.Client.Document
{
    public partial class DocumentSession
    {
        public IEnumerator<object> Stream<T>(IQueryable<T> query)
        {
            var StreamOperation = new StreamOperation(this);
            IEnumerator result = null;
            var command = StreamOperation.CreateRequest(query);
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                result = StreamOperation.SetResult(command.Result);
            }
            while (result.MoveNext())
            {
                var res = (BlittableJsonReaderObject) result.Current;
                string key;
                BlittableJsonReaderObject metadata;
                if (res.TryGet(Constants.Metadata.Key, out metadata))
                    if (metadata.TryGet(Constants.Metadata.Id, out key))
                        yield return ConvertToEntity(typeof(T), key, res);
            }
        }

        public IEnumerator<object> Stream<T>(IDocumentQuery<T> query)
        {
            var StreamOperation = new StreamOperation(this);
            IEnumerator result = null;
            var command = StreamOperation.CreateRequest(query);
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                result = StreamOperation.SetResult(command.Result);
            }
            while (result.MoveNext())
            {
                var res = (BlittableJsonReaderObject)result.Current;
                string key;
                BlittableJsonReaderObject metadata;
                if (res.TryGet(Constants.Metadata.Key, out metadata))
                    if (metadata.TryGet(Constants.Metadata.Id, out key))
                        yield return ConvertToEntity(typeof(T), key, res);
            }
        }

        public IEnumerator<StreamResult> Stream<T>(long? fromEtag, int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string transformer = null,
            Dictionary<string, object> transformerParameters = null)
        {
            throw new NotImplementedException();
            /*public IEnumerator<StreamResult<T>> Stream<T>(long? fromEtag, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null)
       {
           return Stream<T>(fromEtag: fromEtag, startsWith: null, matches: null, start: start, pageSize: pageSize, pagingInformation: pagingInformation, skipAfter: null, transformer: transformer, transformerParameters: transformerParameters);
       }*/
       }

       public IEnumerator<StreamResult> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue,
           RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null,
           Dictionary<string, object> transformerParameters = null)
       {
           throw new NotImplementedException();
            /*public IEnumerator<treamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null)
      {
          return Stream<T>(fromEtag: null, startsWith: startsWith, matches: matches, start: start, pageSize: pageSize, pagingInformation: pagingInformation, skipAfter: skipAfter, transformer: transformer, transformerParameters: transformerParameters);
      }*/
        }

    }
}