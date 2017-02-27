using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Data;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class QueriesController : ClusterAwareRavenDbApiController
    {
        [HttpGet]
        [RavenRoute("queries")]
        [RavenRoute("databases/{databaseName}/queries")]
        public Task<HttpResponseMessage> QueriesGet()
        {
            return GetQueriesResponse(true);
        }

        [HttpPost]
        [RavenRoute("queries")]
        [RavenRoute("databases/{databaseName}/queries")]
        public Task<HttpResponseMessage> QueriesPost()
        {
            return GetQueriesResponse(false);
        }

        private async Task<HttpResponseMessage> GetQueriesResponse(bool isGet)
        {
            RavenJArray itemsToLoad;
            if (isGet == false)
            {
                try
                {
                    itemsToLoad = await ReadJsonArrayAsync().ConfigureAwait(false);
                }
                catch (InvalidOperationException e)
                {
                    if (Log.IsDebugEnabled)
                        Log.DebugException("Failed to deserialize query request." , e);
                    return GetMessageWithObject(new
                    {
                        Message = "Could not understand json, please check its validity."
                    }, (HttpStatusCode)422); //http code 422 - Unprocessable entity

                }
                catch (InvalidDataException e)
                {
                    if (Log.IsDebugEnabled)
                        Log.DebugException("Failed to deserialize query request." , e);
                    return GetMessageWithObject(new
                    {
                        e.Message
                    }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
                }

                AddRequestTraceInfo(sb =>
                {
                    foreach (var item in itemsToLoad)
                    {
                        sb.Append("\t").Append(item).AppendLine();
                    }
                });
            }
            else
            {
                itemsToLoad = new RavenJArray(GetQueryStringValues("id").Cast<object>());
            }

            var result = new MultiLoadResult();
            var loadedIds = new HashSet<string>();
            var includedIds = new HashSet<string>();
            var includes = GetQueryStringValues("include") ?? new string[0];
            var transformer = GetQueryStringValue("transformer") ?? GetQueryStringValue("resultTransformer");
            var transformerParameters = this.ExtractTransformerParameters();
            var transactionInformation = GetRequestTransaction();
            var includedEtags = new List<byte>();

            if (string.IsNullOrEmpty(transformer) == false)
            {
                var transformerDef = Database.IndexDefinitionStorage.GetTransformer(transformer);
                if (transformerDef == null)
                    return GetMessageWithObject(new {Error = "No such transformer: " + transformer}, HttpStatusCode.BadRequest);
                includedEtags.AddRange(transformerDef.GetHashCodeBytes());

            }

            Database.TransactionalStorage.Batch(actions =>
            {
                foreach (RavenJToken item in itemsToLoad)
                {
                    var value = item.Value<string>();
                    if (loadedIds.Add(value) == false)
                        continue;
                    var documentByKey = string.IsNullOrEmpty(transformer)
                                        ? Database.Documents.Get(value, transactionInformation)
                                        : Database.Documents.GetWithTransformer(value, transformer, transactionInformation, transformerParameters, out includedIds);
                    if (documentByKey == null)
                    {
                        if(ClientIsV3OrHigher(Request))
                            result.Results.Add(null); 
                        continue;
                    }
                    result.Results.Add(documentByKey.ToJson());

                    if (documentByKey.Etag != null)
                        includedEtags.AddRange(documentByKey.Etag.ToByteArray());

                    includedEtags.Add((documentByKey.NonAuthoritativeInformation ?? false) ? (byte)0 : (byte)1);
                }

                var addIncludesCommand = new AddIncludesCommand(Database, transactionInformation, (etag, includedDoc, nonAuthorotativeResult) =>
                {
                    includedEtags.AddRange(etag.ToByteArray());
                    includedEtags.Add((byte)(nonAuthorotativeResult ? 1 : 0));
                    result.Includes.Add(includedDoc);
                }, includes, loadedIds);

                foreach (var item in result.Results.Where(item => item != null))
                {
                    addIncludesCommand.Execute(item);
                }
            });


            foreach (var includedId in includedIds)
            {
                var doc = Database.Documents.Get(includedId, transactionInformation);
                if (doc == null)
                {
                    continue;
                }
                includedEtags.AddRange(doc.Etag.ToByteArray());
                result.Includes.Add(doc.ToJson());
            }

            var computeHash = Encryptor.Current.Hash.Compute16(includedEtags.ToArray());
            Etag computedEtag = Etag.Parse(computeHash);

            if (MatchEtag(computedEtag))
            {
                return GetEmptyMessage(HttpStatusCode.NotModified);
            }

            var msg = GetMessageWithObject(result);
            WriteETag(computedEtag, msg);

            AddRequestTraceInfo(sb => sb.AppendFormat("Results count: {0}, includes count: {1}", result.Results.Count, result.Includes.Count).AppendLine());

            return msg;
        }
        
    }
}
