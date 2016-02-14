using System;
using System.Globalization;
using System.Linq;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Microsoft.Extensions.Primitives;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RequestHandler).FullName);

        private RequestHandlerContext _context;

        protected HttpContext HttpContext
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.HttpContext; }
        }

        public RavenServer Server
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RavenServer; }
        }
        public ServerStore ServerStore
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RavenServer.ServerStore; }
        }
        public RouteMatch RouteMatch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RouteMatch; }
        }

        public virtual void Init(RequestHandlerContext context)
        {
            _context = context;
        }

        protected Stream RequestBodyStream()
        {
            var requestBodyStream = HttpContext.Request.Body;

            if(IsGzipRequest()==false)
                return  requestBodyStream;

            var gZipStream = new GZipStream(requestBodyStream, CompressionMode.Decompress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        private bool IsGzipRequest()
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var val in HttpContext.Request.Headers["Content-Encoding"])
            {
                if (val == "gzip")
                    return true;
            }
            return false;
        }

        protected Stream ResponseBodyStream()
        {
            var responseBodyStream = HttpContext.Response.Body;

            if (CanAcceptGzip() == false)
                return responseBodyStream;

            HttpContext.Response.Headers["Content-Encoding"] = "gzip";
            var gZipStream = new GZipStream(responseBodyStream, CompressionMode.Compress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        private bool CanAcceptGzip()
        {
            if (_context.AllowResponseCompression == false)
                return false;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var val in HttpContext.Request.Headers["Accept-Encoding"])
            {
                if (val == "gzip")
                    return true;
            }
            return false;
        }

        protected long? GetLongFromHeaders(string name)
        {
            var etags = HttpContext.Request.Headers[name];
            if (etags.Count == 0)
                return null;

            long etag;
            if (long.TryParse(etags[0], out etag) == false)
                    throw new ArgumentException(
                        "Could not parse header '" + name + "' header as int64, value was: " + etags[0]);
            return etag;
        }

        protected int GetStart(int defaultValue = 0)
        {
            return GetIntQueryString("start", defaultValue);
        }

        protected int GetPageSize(int defaultValue = 25)
        {
            return GetIntQueryString("pageSize", defaultValue);
        }

        protected int GetIntQueryString(string name, int? defaultValue = null)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
            {
                if (defaultValue.HasValue)
                    return defaultValue.Value;
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");
            }

                int result;
                if (int.TryParse(val[0], out result) == false)
                    throw new ArgumentException(
                        string.Format("Could not parse query string '{0}' header as int32, value was: {1}", name, val[0]));
                return result;
            }


        protected long GetLongQueryString(string name)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

            long result;
            if (long.TryParse(val[0], out result) == false)
                throw new ArgumentException(
                    string.Format("Could not parse query string '{0}' header as int64, value was: {1}", name, val[0]));
            return result;
        }

        protected string GetStringQueryString(string name, bool required = false)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

            return val[0];
        }

        protected StringValues GetStringValuesQueryString(string name)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

            return val;
        }

        protected virtual IndexQuery GetIndexQuery(int maxPageSize)
        {
            var query = new IndexQuery
            {
                Query = GetStringQueryString("query") ?? /* TODO arek queryFromPostRequest ?? */"",
                Start = GetStart(),
                //Cutoff = GetCutOff(),
                //WaitForNonStaleResultsAsOfNow = GetWaitForNonStaleResultsAsOfNow(),
                //CutoffEtag = GetCutOffEtag(),
                PageSize = GetPageSize(maxPageSize),
                FieldsToFetch = GetStringValuesQueryString("fetch").ToArray(),
                DefaultField = GetStringQueryString("defaultField"),

                DefaultOperator =
                    string.Equals(GetStringQueryString("operator"), "AND", StringComparison.OrdinalIgnoreCase) ?
                        QueryOperator.And :
                        QueryOperator.Or,

                SortedFields = EnumerableExtension.EmptyIfNull(GetStringValuesQueryString("sort"))
                    .Select(x => new SortedField(x))
                    .ToArray(),
                //HighlightedFields = GetHighlightedFields().ToArray(),
                HighlighterPreTags = GetStringValuesQueryString("preTags").ToArray(),
                HighlighterPostTags = GetStringValuesQueryString("postTags").ToArray(),
                HighlighterKeyName = GetStringQueryString("highlighterKeyName"),
                ResultsTransformer = GetStringQueryString("resultsTransformer"),
                //TransformerParameters = ExtractTransformerParameters(),
                //ExplainScores = GetExplainScores(),
                //SortHints = GetSortHints(),
                //IsDistinct = IsDistinct()
            };

            //var allowMultipleIndexEntriesForSameDocumentToResultTransformer = GetQueryStringValue("allowMultipleIndexEntriesForSameDocumentToResultTransformer");
            //bool allowMultiple;
            //if (string.IsNullOrEmpty(allowMultipleIndexEntriesForSameDocumentToResultTransformer) == false && bool.TryParse(allowMultipleIndexEntriesForSameDocumentToResultTransformer, out allowMultiple))
            //    query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultiple;

            //if (query.WaitForNonStaleResultsAsOfNow)
            //    query.Cutoff = SystemTime.UtcNow;

            //var showTimingsAsString = GetQueryStringValue("showTimings");
            //bool showTimings;
            //if (string.IsNullOrEmpty(showTimingsAsString) == false && bool.TryParse(showTimingsAsString, out showTimings) && showTimings)
            //    query.ShowTimings = true;

            //var skipDuplicateCheckingAsstring = GetQueryStringValue("skipDuplicateChecking");
            //bool skipDuplicateChecking;
            //if (string.IsNullOrEmpty(skipDuplicateCheckingAsstring) == false &&
            //    bool.TryParse(skipDuplicateCheckingAsstring, out skipDuplicateChecking) && skipDuplicateChecking)
            //    query.ShowTimings = true;

            //var spatialFieldName = GetQueryStringValue("spatialField") ?? Constants.DefaultSpatialFieldName;
            //var queryShape = GetQueryStringValue("queryShape");
            //SpatialUnits units;
            //var unitsSpecified = Enum.TryParse(GetQueryStringValue("spatialUnits"), out units);
            //double distanceErrorPct;
            //if (!double.TryParse(GetQueryStringValue("distErrPrc"), NumberStyles.Any, CultureInfo.InvariantCulture, out distanceErrorPct))
            //    distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
            //SpatialRelation spatialRelation;

            //if (Enum.TryParse(GetQueryStringValue("spatialRelation"), false, out spatialRelation) && !string.IsNullOrWhiteSpace(queryShape))
            //{
            //    return new SpatialIndexQuery(query)
            //    {
            //        SpatialFieldName = spatialFieldName,
            //        QueryShape = queryShape,
            //        RadiusUnitOverride = unitsSpecified ? units : (SpatialUnits?)null,
            //        SpatialRelation = spatialRelation,
            //        DistanceErrorPercentage = distanceErrorPct,
            //    };
            //}

            return query;
        }
    }
}