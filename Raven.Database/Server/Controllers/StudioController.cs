using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    
    [RoutePrefix("")]
    public class StudioController : BaseDatabaseApiController
    {
        private static readonly string[] FieldsToTake = { "@id", Constants.RavenEntityName };

        public const int DocPreviewMaxColumns = 9;

        public const int DocPreviewColumnTextLimit = 256;

        [HttpGet]
        [RavenRoute("raven")]
        [RavenRoute("raven/{*id}")]
        public HttpResponseMessage RavenUiGet(string id = null)
        {
            if (string.IsNullOrEmpty(Database.Configuration.RedirectStudioUrl) == false)
            {
                var result = InnerRequest.CreateResponse(HttpStatusCode.Found);
                result.Headers.Location = new Uri(Path.Combine(DatabasesLandlord.SystemConfiguration.ServerUrl, Database.Configuration.RedirectStudioUrl));
                return result;
            }

            var docPath = GetRequestUrl().Replace("/raven/", "");
            return WriteEmbeddedFile(DatabasesLandlord.SystemConfiguration.WebDir, "Raven.Database.Server.WebUI", null, docPath);
        }

        [HttpGet]
        [RavenRoute("studio")]
        [RavenRoute("studio/{*path}")]
        public HttpResponseMessage GetStudioFile(string path = null)
        {
            if (string.IsNullOrEmpty(Database.Configuration.RedirectStudioUrl) == false)
            {
                var result = InnerRequest.CreateResponse(HttpStatusCode.Found);
                result.Headers.Location = new Uri(Path.Combine(DatabasesLandlord.SystemConfiguration.ServerUrl, Database.Configuration.RedirectStudioUrl));
                return result;
            }

            var url = GetRequestUrl();
            var docPath = url.StartsWith("/studio/") ? url.Substring("/studio/".Length) : url;
            return WriteEmbeddedFile("~/Server/Html5Studio", "Raven.Database.Server.Html5Studio", "Raven.Studio.Html5", docPath);
        }

        [HttpGet]
        [RavenRoute("doc-preview")]
        [RavenRoute("databases/{databaseName}/doc-preview")]
        public HttpResponseMessage GetDocumentsPreview(string collection = null)
        {
            using (var cts = new CancellationTokenSource())
            using (cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
                try
                {
                    List<RavenJObject> results;
                    int totalResults;
                    var statusCode = HttpStatusCode.OK;
                    var start = GetStart();
                    var pageSize = GetPageSize(Database.Configuration.MaxPageSize);

                    if (string.IsNullOrEmpty(collection))
                    {
                        var totalCountQuery = Database.Queries.Query(Constants.DocumentsByEntityNameIndex, new IndexQuery
                        {
                            Start = 0,
                            PageSize = 0
                        }, cts.Token);
                        totalResults = totalCountQuery.TotalResults;
                        
                        results = new List<RavenJObject>(pageSize);
                        Database.Documents.GetDocuments(start, pageSize, GetEtagFromQueryString(), cts.Token, doc =>
                        {
                            if (doc != null) results.Add(doc.ToJson());
                            return true;
                        });
                    }
                    else
                    {
                        var indexQuery = new IndexQuery
                        {
                            Query = "Tag:" + RavenQuery.Escape(collection),
                            Start = start,
                            PageSize = pageSize,
                            SortedFields = new[] { new SortedField("-LastModifiedTicks") }
                        };

                        var queryResult = Database.Queries.Query(Constants.DocumentsByEntityNameIndex, indexQuery, cts.Token);

                        Database.IndexStorage.SetLastQueryTime(queryResult.IndexName, queryResult.LastQueryTime);

                        totalResults = queryResult.TotalResults;
                        results = queryResult.Results;
                        if (queryResult.NonAuthoritativeInformation)
                        {
                            statusCode = HttpStatusCode.NonAuthoritativeInformation;
                        }
                    }

                    var bindings = GetQueryStringValues("binding");

                    if (bindings.Length > 0)
                    {
                        var bindingGroups = BindingsHelper.AnalyzeBindings(bindings);

                        return GetMessageWithObject(new
                        {
                            TotalResults = totalResults,
                            Results = TrimContents(results, bindingGroups)
                        }, statusCode);
                    }
                    else
                    {
                        // since user does not specified colums/bindings to use to sample input data to find
                        // columns that we use
                        var columns = SampleColumnNames(results);
                        return GetMessageWithObject(new
                        {
                            TotalResults = totalResults,
                            Results = TrimContents(results, new BindingGroups
                            {
                                SimpleBindings = columns
                            })
                        }, statusCode);
                    }
                }
                catch (OperationCanceledException e)
                {
                    throw new TimeoutException(string.Format("The query did not produce results in {0}", DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout), e);
                }
            }
        }

        private List<RavenJObject> TrimContents(List<RavenJObject> input, BindingGroups bindingGroups)
        {
            var result = new List<RavenJObject>();

            foreach (var jObject in input)
            {

                var metadata = jObject.Value<RavenJObject>(Constants.Metadata);

                var filteredMetadata = new RavenJObject();

                foreach (var field in FieldsToTake)
                {
                    filteredMetadata[field] = metadata.Value<string>(field);
                }

                var filteredObject = new RavenJObject
                {
                    [Constants.Metadata] = filteredMetadata
                };

                if (bindingGroups.SimpleBindings != null)
                {
                    foreach (var column in bindingGroups.SimpleBindings)
                    {
                        RavenJToken value;
                        if (jObject.TryGetValue(column, out value))
                        {
                            var jValue = value as RavenJValue;
                            if (jValue != null && (jValue.Type == JTokenType.Float || jValue.Type == JTokenType.Integer))
                            {
                                filteredObject[column] = jValue;
                            }
                            else
                            {
                                var valueAsString = value.ToString();
                                filteredObject[column] = valueAsString.Length > DocPreviewColumnTextLimit ? valueAsString.Substring(0, DocPreviewColumnTextLimit) + "..." : valueAsString;
                            }
                        }
                    }
                }

                if (bindingGroups.CompoundBindings != null)
                {
                    foreach (var column in bindingGroups.CompoundBindings)
                    {
                        RavenJToken value;
                        if (jObject.TryGetValue(column, out value))
                        {
                            filteredObject[column] = value;
                            
                        }
                    }
                }

                result.Add(filteredObject);
            }

            return result;
        }

        /// <summary>
        /// Iterate over input until you find up to $columnLimit distinct column names
        /// </summary>
        private List<string> SampleColumnNames(List<RavenJObject> input)
        {
            var columns = new List<string>();

            foreach (var jObject in input)
            {
                foreach (var kvp in jObject)
                {
                    if (kvp.Key != Constants.Metadata && !columns.Contains(kvp.Key))
                    {
                        columns.Add(kvp.Key);
                        if (columns.Count == DocPreviewMaxColumns)
                        {
                            return columns;
                        }
                    }
                }
            }

            return columns;
        }
    }
}
