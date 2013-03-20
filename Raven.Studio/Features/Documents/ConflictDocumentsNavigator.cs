using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Features.Documents
{
    public class ConflictDocumentsNavigator : DocumentNavigator
    {
        private const string ConflictsIndexName = "Raven/ConflictDocuments";

        private readonly string id;
        private readonly int itemIndex;

        public ConflictDocumentsNavigator(string id, int itemIndex)
        {
            this.id = id;
            this.itemIndex = itemIndex;
        }

        public override string GetUrl()
        {
            var builder = GetUrlBuilder(itemIndex);

            builder.SetQueryParam("id", id);

            return builder.BuildUrl();
        }

	    public override Task<DocumentAndNavigationInfo> GetDocument()
	    {
		    if (string.IsNullOrEmpty(id))
		    {
			    return
				    QueryIndexForDocument()
					    .ContinueWith(
						    t =>
						    {
							    var info = PopulateDocumentOrConflictsFromDocuments(t.Result.Results.Select(r => r.ToJsonDocument()));
							    info.TotalDocuments = t.Result.TotalResults;
							    info.Index = itemIndex;
							    info.ParentPath = GetParentPath();
							    info.UrlForFirst = GetUrlForIndex(0);
							    info.UrlForPrevious = itemIndex > 0 ? GetUrlForIndex(itemIndex - 1) : null;
							    info.UrlForNext = itemIndex < t.Result.TotalResults - 1 ? GetUrlForIndex(itemIndex + 1) : null;
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
            var query = new IndexQuery {Start = itemIndex, PageSize = 1, SkipTransformResults = true};

		    return DatabaseCommands.QueryAsync(ConflictsIndexName, query, null);
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

            builder.SetQueryParam("navigationMode", "conflicts");
            builder.SetQueryParam("itemIndex", index);

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
	        return new[]
	        {
		        new PathSegment {Name = "Documents", Url = "/Documents"},
		        new PathSegment {Name = "Conflicts", Url = "/conflicts"}
	        };
        }
        
        public static DocumentNavigator ConflictsNavigatorFromUrl(UrlParser parser)
        {
            var id = parser.GetQueryParam("id");

            int itemIndex;
            int.TryParse(parser.GetQueryParam("itemIndex"), out itemIndex);

            return new ConflictDocumentsNavigator(id, itemIndex);
        }
    }
}