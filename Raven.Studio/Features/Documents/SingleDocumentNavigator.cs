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

namespace Raven.Studio.Features.Documents
{
    public class SingleDocumentNavigator : DocumentNavigator
    {
        private string id;

        public SingleDocumentNavigator(string id)
        {
            this.id = id;
        }

        public override string GetUrl()
        {
            var urlBuilder = GetBaseUrl();

            urlBuilder.SetQueryParam("id", id);

            return urlBuilder.BuildUrl();
        }

        public override Task<DocumentAndNavigationInfo> GetDocument()
        {
            return DatabaseCommands.GetAsync(id).ContinueWith(t => new DocumentAndNavigationInfo()
                                                                       {
                                                                           Document = t.Result,
                                                                           TotalDocuments = 1,
                                                                           Index = 0,
                                                                           ParentPath = GetParentPath(t.Result),
                                                                       });
        }

        public override string GetUrlForCurrentIndex()
        {
            return base.GetUrlForCurrentIndex();
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
    }
}
