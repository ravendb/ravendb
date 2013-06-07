using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Framework;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class AllDocumentsNavigator : DocumentNavigator
    {
        private readonly string id;
        private readonly int itemIndex;

        public AllDocumentsNavigator(string id, int itemIndex)
        {
            this.id = id;
            this.itemIndex = itemIndex;
        }

        public override string GetUrl()
        {
            var builder = GetBaseUrl();

            builder.SetQueryParam("id", id);
            builder.SetQueryParam("navigationMode", "allDocs");
            builder.SetQueryParam("itemIndex", itemIndex);

            return builder.BuildUrl();
        }

	    public override Task<DocumentAndNavigationInfo> GetDocument()
	    {
		    if (string.IsNullOrEmpty(id))
		    {
			    return DatabaseCommands.GetDocumentsAsync(itemIndex, 1)
			                           .ContinueWith(t =>
			                           {
				                           var totalDocuments = GetTotalDocuments();
				                           var info = PopulateDocumentOrConflictsFromDocuments(t.Result);
				                           info.TotalDocuments = totalDocuments;
				                           info.Index = itemIndex;
				                           info.ParentPath = GetParentPath(info);
				                           info.UrlForFirst = GetUrlForIndex(0);
				                           info.UrlForPrevious = itemIndex > 0 ? GetUrlForIndex(itemIndex - 1) : null;
				                           info.UrlForNext = itemIndex < totalDocuments - 1 ? GetUrlForIndex(itemIndex + 1) : null;
				                           info.UrlForLast = GetUrlForIndex(totalDocuments - 1);
				                           return info;
			                           }
				    );
		    }

		    return DatabaseCommands.GetAsync(id).ContinueWith
			    (t =>
			    {
				    var totalDocuments = GetTotalDocuments();
				    var info = PopulateDocumentOrConflictsFromTask(t, id);

				    info.TotalDocuments = GetTotalDocuments();
				    info.Index = itemIndex;
				    info.ParentPath = GetParentPath(info);
				    info.UrlForFirst = GetUrlForIndex(0);
				    info.UrlForPrevious = itemIndex > 0 ? GetUrlForIndex(itemIndex - 1) : null;
				    info.UrlForNext = itemIndex < totalDocuments - 1 ? GetUrlForIndex(itemIndex + 1) : null;
				    info.UrlForLast = GetUrlForIndex(totalDocuments - 1);
				    return info;
			    });
	    }

	    private static long GetTotalDocuments()
        {
            // since we're working on a background thread, we have to be prepare for observable to change underneath us.
            var databaseModel = ApplicationModel.Database.Value;
            if (databaseModel == null)
                return 0;

            var databaseStatistics = databaseModel.Statistics.Value;
            if (databaseStatistics == null)
                return 0;

            return databaseStatistics.CountOfDocuments;
        }

        public override string GetUrlForNext()
        {
            return GetUrlForIndex(itemIndex + 1);
        }

        public override string GetUrlForPrevious()
        {
            return GetUrlForIndex(itemIndex - 1);
        }

        public override string GetUrlForCurrentIndex()
        {
            return GetUrlForIndex(itemIndex);
        }

        protected IList<PathSegment> GetParentPath(DocumentAndNavigationInfo info)
        {
            if (info.IsConflicted)
            {
                return new[]
                           {
                               new PathSegment {Name = "Documents", Url = "/documents"},
                               new PathSegment {Name = "Conflicts", Url = "/conflicts"}
                           };
            }

            if (info.Document == null)
            {
                return null;
            }

            var entityType = info.Document.Metadata.IfPresent<string>(Constants.RavenEntityName);

            if (entityType != null)
            {
                return new[]
                           {
                               new PathSegment {Name = "Documents", Url = "/documents"},
                               new PathSegment {Name = entityType, Url = "/documents?collection=" + entityType}
                           };
            }

	        return new[] {new PathSegment {Name = "Documents", Url = "/documents"}};
        }

        private string GetUrlForIndex(long index)
        {
            var builder = GetBaseUrl();

            builder.SetQueryParam("navigationMode", "allDocs");
            builder.SetQueryParam("itemIndex", index);

            return builder.BuildUrl();
        }

        public static DocumentNavigator AllDocumentsFromUrl(UrlParser parser)
        {
            var id = parser.GetQueryParam("id");

            int itemIndex;
            int.TryParse(parser.GetQueryParam("itemIndex"), out itemIndex);

            return new AllDocumentsNavigator(id, itemIndex);
        }
    }
}