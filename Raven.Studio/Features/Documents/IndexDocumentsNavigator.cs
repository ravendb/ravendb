using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Features.Documents
{
    public class IndexDocumentsNavigator : DocumentNavigator
    {
        private readonly string id;
        private readonly int itemIndex;
        private readonly string indexName;
        private readonly IndexQuery templateQuery;

        public IndexDocumentsNavigator(string id, int itemIndex, string indexName, IndexQuery templateQuery)
        {
            this.id = id;
            this.itemIndex = itemIndex;
            this.indexName = indexName;

            // canonicalize the template query to help keep the url more stable
            templateQuery.Start = 0;
            templateQuery.PageSize = 1;

            this.templateQuery = templateQuery;
        }

        public override string GetUrl()
        {
            var builder = GetUrlBuilder(itemIndex);

            builder.SetQueryParam("id", id);

            return builder.BuildUrl();
        }

        private string GetQueryString()
        {
            return templateQuery.GetQueryString().TrimStart('?');
        }

	    public override Task<DocumentAndNavigationInfo> GetDocument()
	    {
		    if (string.IsNullOrEmpty(id))
		    {
			    var query = templateQuery.Clone();
			    query.Start = itemIndex;
			    query.PageSize = 1;

			    return DatabaseCommands.QueryAsync(indexName, query, null)
			                           .ContinueWith(
				                           t =>
				                           {
					                           var info = PopulateDocumentOrConflictsFromDocuments(
						                           t.Result.Results.Select(r => r.ToJsonDocument()));

					                           info.TotalDocuments = t.Result.TotalResults;
					                           info.Index = itemIndex;
					                           info.ParentPath = GetParentPath();
					                           info.UrlForFirst = GetUrlForIndex(0);
					                           info.UrlForPrevious = itemIndex > 0 ? GetUrlForIndex(itemIndex - 1) : null;
					                           info.UrlForNext = itemIndex < t.Result.TotalResults - 1
						                                             ? GetUrlForIndex(itemIndex + 1)
						                                             : null;
					                           info.UrlForLast = GetUrlForIndex(t.Result.TotalResults - 1);
					                           return info;
				                           });
		    }

		    var getDocumentTask = DatabaseCommands.GetAsync(id);
		    var getStatisticsTask = QueryIndexForDocument();

		    return TaskEx.WhenAll(getDocumentTask, getStatisticsTask)
		                 .ContinueWith(_ =>
		                 {
			                 var info = PopulateDocumentOrConflictsFromTask(getDocumentTask, id);
			                 info.Index = itemIndex;
			                 info.TotalDocuments = getStatisticsTask.Result.TotalResults;
			                 info.ParentPath = GetParentPath();
			                 info.UrlForFirst = GetUrlForIndex(0);
			                 info.UrlForPrevious = itemIndex > 0 ? GetUrlForIndex(itemIndex - 1) : null;
			                 info.UrlForNext = itemIndex < getStatisticsTask.Result.TotalResults - 1
				                                   ? GetUrlForIndex(itemIndex + 1)
				                                   : null;
			                 info.UrlForLast = GetUrlForIndex(getStatisticsTask.Result.TotalResults - 1);

			                 return info;
		                 }
			    );
	    }

	    private Task<QueryResult> QueryIndexForDocument()
        {
            var query = templateQuery.Clone();
            query.Start = itemIndex;
            query.PageSize = 1;

            return DatabaseCommands.QueryAsync(indexName, query, null);
        }

        public override string GetUrlForNext()
        {
            return GetUrlForIndex(itemIndex + 1);
        }

        private string GetUrlForIndex(int index)
        {
            var builder = GetUrlBuilder(index);

            return builder.BuildUrl();
        }

        private UrlParser GetUrlBuilder(int index)
        {
            var builder = GetBaseUrl();

            builder.SetQueryParam("navigationMode", "index");
            builder.SetQueryParam("itemIndex", index);
            builder.SetQueryParam("indexName", indexName);
            builder.SetQueryParam("query", GetQueryString());

            return builder;
        }

        public override string GetUrlForPrevious()
        {
            return GetUrlForIndex(itemIndex - 1);
        }

        public override string GetUrlForCurrentIndex()
        {
            return GetUrlForIndex(itemIndex);
        }

        protected IList<PathSegment> GetParentPath()
        {
            if (indexName == "Raven/DocumentsByEntityName")
            {
                var collectionName = GetCollectionName(templateQuery);
                if (collectionName != null)
                {
	                return new[]
	                {
		                new PathSegment {Name = "Documents", Url = "/documents"},
		                new PathSegment {Name = collectionName, Url = "/documents?collection=" + collectionName}
	                };
                }
            }

	        return new[]
	        {
		        new PathSegment {Name = "Indexes", Url = "/Indexes"},
		        new PathSegment {Name = indexName, Url = "/indexes/" + indexName},
		        new PathSegment {Name = "Query", Url = "/query/" + indexName},
		        new PathSegment {Name = "Reporting", Url = "/reporting/" + indexName},
	        };
        }

        private string GetCollectionName(IndexQuery indexQuery)
        {
            if (indexQuery == null || indexQuery.Query == null)
                return null;

            var matches = Regex.Match(indexQuery.Query, @"Tag:(\w+)");
            if (matches.Success)
                return matches.Groups[1].Value;

            return null;
        }

        public static DocumentNavigator IndexNavigatorFromUrl(UrlParser parser)
        {
            var id = parser.GetQueryParam("id");

            int itemIndex;
            int.TryParse(parser.GetQueryParam("itemIndex"), out itemIndex);

            var queryString = parser.GetQueryParam("query");
            var query = string.IsNullOrEmpty(queryString) ? new IndexQuery() : IndexQueryHelpers.FromQueryString(queryString);

            return new IndexDocumentsNavigator(id, itemIndex, parser.GetQueryParam("indexName"), query);
        }
    }
}