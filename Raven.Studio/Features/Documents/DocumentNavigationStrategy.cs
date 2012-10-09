using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class DocumentAndNavigationInfo
    {
        public JsonDocument Document { get; set; }
        public long Index { get; set; }
        public long TotalDocuments { get; set; }
        public IList<PathSegment> ParentPath { get; set; }
        public string UrlForFirst { get; set; }
        public string UrlForPrevious { get; set; }
        public string UrlForNext { get; set; }
        public string UrlForLast { get; set; }
    }

    public class PathSegment
    {
        public string Name { get; set; }
        public string Url { get; set; }
    }

    public abstract class DocumentNavigator
    {
        public static DocumentNavigator Create(string id)
        {
            return new SingleDocumentNavigator(id);
        }

        public static DocumentNavigator Create(string id, int itemIndex)
        {
            return new AllDocumentsNavigator(id, itemIndex);
        }

        public static DocumentNavigator Create(string id, int itemIndex, string indexName, IndexQuery query)
        {
            return new IndexDocumentsNavigator(id, itemIndex, indexName, query);
        }

        public static DocumentNavigator FromUrl(UrlParser parser)
        {
            var mode = parser.GetQueryParam("navigationMode");

            if (mode == "allDocs")
                return AllDocumentsNavigator.AllDocumentsFromUrl(parser);
            
            if (mode == "index")
                return IndexDocumentsNavigator.IndexNavigatorFromUrl(parser);

            return new SingleDocumentNavigator(parser.GetQueryParam("id"));
        }

        public abstract string GetUrl();

        public abstract Task<DocumentAndNavigationInfo> GetDocument();

        public virtual string GetUrlForNext()
        {
            return string.Empty;
        }

        public virtual string GetUrlForPrevious()
        {
            return string.Empty;
        }

        public virtual string GetUrlForCurrentIndex()
        {
            return string.Empty;
        }

        protected UrlParser GetBaseUrl()
        {
            return new UrlParser("/edit");
        }

        protected IAsyncDatabaseCommands DatabaseCommands
        {
            get { return ApplicationModel.DatabaseCommands; }
        }
    }
}