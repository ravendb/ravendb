using System;
using System.Collections.Specialized;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Queries;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
    public class MoreLikeThisController : ClusterAwareRavenDbApiController
    {
        [HttpGet]
        [RavenRoute("morelikethis/{*id}")]
        [RavenRoute("databases/{databaseName}/morelikethis/{*id}")]
        public HttpResponseMessage MoreLikeThisGet()
        {
            var nameValueCollection = new NameValueCollection();
            foreach (var queryNameValuePair in InnerRequest.GetQueryNameValuePairs())
            {
                nameValueCollection.Add(queryNameValuePair.Key, queryNameValuePair.Value);
            }

            var parameters = GetParametersFromPath(GetRequestUrl(), nameValueCollection);
            parameters.TransformerParameters = ExtractTransformerParameters();
            parameters.ResultsTransformer = GetQueryStringValue("resultsTransformer");
            parameters.Includes = GetQueryStringValues("include");

            var index = Database.IndexStorage.GetIndexInstance(parameters.IndexName);
            if (index == null)
            {
                return GetMessageWithObject(new { Error = "The index " + parameters.IndexName + " cannot be found" },
                    HttpStatusCode.NotFound);
            }

            var indexEtag = Database.Indexes.GetIndexEtag(parameters.IndexName, null);
            if (MatchEtag(indexEtag))
                return GetEmptyMessage(HttpStatusCode.NotModified);


            var result = Database.ExecuteMoreLikeThisQuery(parameters, GetRequestTransaction(), GetPageSize(Database.Configuration.MaxPageSize));

            if (MatchEtag(result.Etag))
                return GetEmptyMessage(HttpStatusCode.NotModified);

            var msg = GetMessageWithObject(result.Result);
            WriteETag(result.Etag, msg);
            return msg;
        }

        public static MoreLikeThisQuery GetParametersFromPath(string path, NameValueCollection query)
        {
            var results = new MoreLikeThisQuery
            {
                IndexName = query.Get("index"),
                Fields = query.GetValues("fields"),
                Boost = query.Get("boost").ToNullableBool(),
                BoostFactor = query.Get("boostFactor").ToNullableFloat(),
                MaximumNumberOfTokensParsed = query.Get("maxNumTokens").ToNullableInt(),
                MaximumQueryTerms = query.Get("maxQueryTerms").ToNullableInt(),
                MaximumWordLength = query.Get("maxWordLen").ToNullableInt(),
                MinimumDocumentFrequency = query.Get("minDocFreq").ToNullableInt(),
                MaximumDocumentFrequency = query.Get("maxDocFreq").ToNullableInt(),
                MaximumDocumentFrequencyPercentage = query.Get("maxDocFreqPct").ToNullableInt(),
                MinimumTermFrequency = query.Get("minTermFreq").ToNullableInt(),
                MinimumWordLength = query.Get("minWordLen").ToNullableInt(),
                StopWordsDocumentId = query.Get("stopWords"),
                AdditionalQuery = query.Get("query"),
                DefaultAnalyzerName = query.Get("defaultAnalyzer"),
                Document = query.Get("document")
            };

            if (string.IsNullOrEmpty(query.Get("docid")) == false)
            {
                var keyValues = query.Get("docid").Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var keyValue in keyValues)
                {
                    var split = keyValue.IndexOf('=');

                    if (split >= 0)
                        results.MapGroupFields.Add(keyValue.Substring(0, split), keyValue.Substring(split + 1));
                    else
                        results.DocumentId = keyValue;
                }
            }

            return results;
        }
    }

    internal static class StringConverter
    {
        public static int? ToNullableInt(this string value)
        {
            int ret;
            if (value == null || !int.TryParse(value, out ret)) return null;
            return ret;
        }

        public static bool? ToNullableBool(this string value)
        {
            bool ret;
            if (value == null || !bool.TryParse(value, out ret)) return null;
            return ret;
        }

        public static float? ToNullableFloat(this string value)
        {
            float ret;
            if (value == null || !float.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out ret)) return null;
            return ret;
        }
    }
}
