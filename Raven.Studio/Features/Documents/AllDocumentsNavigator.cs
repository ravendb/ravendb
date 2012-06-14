using System;
using System.Collections.Generic;
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
                                          return new DocumentAndNavigationInfo
                                              {
                                                  TotalDocuments = totalDocuments,
                                                  Index = itemIndex,
                                                  Document = t.Result.Length > 0 ? t.Result[0] : null,
                                                  ParentPath = GetParentPath(t.Result[0]),
                                                  UrlForFirst = GetUrlForIndex(0),
                                                  UrlForPrevious = itemIndex > 0 ? GetUrlForIndex(itemIndex - 1) : null,
                                                  UrlForNext =
                                                      itemIndex < totalDocuments - 1
                                                          ? GetUrlForIndex(itemIndex + 1)
                                                          : null,
                                                  UrlForLast = GetUrlForIndex(totalDocuments - 1),
                                              };
                                      }
                    );
            }
            else
            {
                return DatabaseCommands.GetAsync(id).ContinueWith
                    (t =>
                         {
                             var totalDocuments = GetTotalDocuments();
                             return new DocumentAndNavigationInfo
                                        {
                                            TotalDocuments = GetTotalDocuments(),
                                            Index = itemIndex,
                                            Document = t.Result,
                                            ParentPath = GetParentPath(t.Result),
                                            UrlForFirst = GetUrlForIndex(0),
                                            UrlForPrevious = itemIndex > 0 ? GetUrlForIndex(itemIndex - 1) : null,
                                            UrlForNext =
                                                itemIndex < totalDocuments - 1
                                                    ? GetUrlForIndex(itemIndex + 1)
                                                    : null,
                                            UrlForLast = GetUrlForIndex(totalDocuments - 1),
                                        };
                         }
                    );
            }
        }

        private static long GetTotalDocuments()
        {
            return ApplicationModel.Database.Value == null
                       ? 0
                       : ApplicationModel.Database.Value.Statistics == null
                             ? 0
                             : ApplicationModel.Database.Value.Statistics.Value.CountOfDocuments;
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

        protected IList<PathSegment> GetParentPath(JsonDocument result)
        {
            if (result == null)
            {
                return null;
            }
            var entityType = result.Metadata.IfPresent<string>(Constants.RavenEntityName);

            if (entityType != null)
            {
                return new[]
                           {
                               new PathSegment() {Name = "Documents", Url = "/documents"},
                               new PathSegment()
                                   {Name = entityType, Url = "/collections?name=" + entityType}
                           };
            }

            return new[]
                       {
                           new PathSegment() { Name = "Documents", Url = "/documents"}
                       };
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
