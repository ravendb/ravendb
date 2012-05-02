using System;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;

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
            var builder = GetBaseUrl();

            builder.SetQueryParam("id", id);
            builder.SetQueryParam("navigationMode", "index");
            builder.SetQueryParam("itemIndex", itemIndex);
            builder.SetQueryParam("indexName", indexName);
            builder.SetQueryParam("query", GetQueryString());

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

                return
                    DatabaseCommands.QueryAsync(indexName, query, null)
                        .ContinueWith(
                            t => new DocumentAndNavigationInfo()
                                     {
                                         Document = t.Result.Results.Count > 0 ? t.Result.Results[0].ToJsonDocument() : null,
                                         TotalDocuments = t.Result.TotalResults,
                                         Index = itemIndex
                                     });
            }
            else
            {
                var getDocumentTask = DatabaseCommands.GetAsync(id);
                var getStatisticsTask = QueryIndexForDocument();

                return TaskEx.WhenAll(getDocumentTask, getStatisticsTask)
                    .ContinueWith(_ =>
                                  new DocumentAndNavigationInfo()
                                      {
                                          Document = getDocumentTask.Result,
                                          Index = itemIndex,
                                          TotalDocuments = getStatisticsTask.Result.TotalResults,
                                      }
                    );
            }
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
            var builder = GetBaseUrl();

            builder.SetQueryParam("navigationMode", "index");
            builder.SetQueryParam("itemIndex", itemIndex + 1);
            builder.SetQueryParam("indexName", indexName);
            builder.SetQueryParam("query", GetQueryString());

            return builder.BuildUrl();
        }

        public override string GetUrlForPrevious()
        {
            var builder = GetBaseUrl();

            builder.SetQueryParam("navigationMode", "index");
            builder.SetQueryParam("itemIndex", itemIndex - 1);
            builder.SetQueryParam("indexName", indexName);
            builder.SetQueryParam("query", GetQueryString());

            return builder.BuildUrl();
        }

        public static DocumentNavigator IndexNavigatorFromUrl(UrlParser parser)
        {
            var id = parser.GetQueryParam("id");

            int itemIndex;
            int.TryParse(parser.GetQueryParam("itemIndex"), out itemIndex);

            var queryString = parser.GetQueryParam("query");
            IndexQuery query;
            if (string.IsNullOrEmpty(queryString))
            {
                query = new IndexQuery();
            }
            else
            {
                query = IndexQueryHelpers.FromQueryString(queryString);
            }

            return new IndexDocumentsNavigator(id, itemIndex, parser.GetQueryParam("indexName"), query);
        }
    }
}
