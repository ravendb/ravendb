using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class DocumentsCollectionSource : DocumentsVirtualCollectionSourceBase
    {
        public DocumentsCollectionSource()
        {
        }

        private void UpdateCount()
        {
            ApplicationModel.DatabaseCommands.GetStatisticsAsync().ContinueWith(t => SetCount((int)t.Result.CountOfDocuments)).Catch();
        }
        
        protected override Task<IList<ViewableDocument>> GetPageAsyncOverride(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            if (!Count.HasValue)
            {
                UpdateCount();
            }

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