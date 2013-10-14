using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Studio.Framework;

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
            return DatabaseCommands.GetAsync(id).ContinueWith(t =>
            {
                var info = PopulateDocumentOrConflictsFromTask(t, id);

                info.TotalDocuments = 1;
                info.Index = 0;
                info.ParentPath = GetParentPath(info);

                return info;
            });
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

	        return new[]
	        {
		        new PathSegment {Name = "Documents", Url = "/documents"}
	        };
        }
    }
}