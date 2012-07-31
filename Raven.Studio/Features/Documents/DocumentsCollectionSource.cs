using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Studio.Extensions;

namespace Raven.Studio.Features.Documents
{
    public class DocumentsCollectionSource : DocumentsVirtualCollectionSourceBase
    {
        public DocumentsCollectionSource()
        {
        }

        protected override Task<int> GetCount()
        {
            return ApplicationModel.DatabaseCommands.GetStatisticsAsync().ContinueWith(t => (int)t.Result.CountOfDocuments);
        }

        protected override Task<IList<ViewableDocument>> GetPageAsyncOverride(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            return ApplicationModel.DatabaseCommands.GetDocumentsAsync(start, pageSize, MetadataOnly)
                .ContinueWith(t =>
                {
                    var docs = (IList<ViewableDocument>)t.Result.Select(x => new ViewableDocument(x)).ToArray();
                    return docs;
                })
                .Catch();
        }
    }
}
