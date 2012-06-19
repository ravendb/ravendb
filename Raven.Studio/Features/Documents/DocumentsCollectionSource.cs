using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class DocumentsCollectionSource : VirtualCollectionSource<ViewableDocument>
    {
        public DocumentsCollectionSource()
        {
        }

        protected override Task<int> GetCount()
        {
            if (ApplicationModel.Database.Value != null
                && ApplicationModel.Database.Value.Statistics.Value != null)
            {
                return TaskEx.FromResult((int) ApplicationModel.Database.Value.Statistics.Value.CountOfDocuments);
            }
            else
            {
                return TaskEx.FromResult(0);
            }
        }

        public override Task<IList<ViewableDocument>> GetPageAsync(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            return ApplicationModel.DatabaseCommands.GetDocumentsAsync(start, pageSize)
                .ContinueWith(t =>
                {
                    var docs = (IList<ViewableDocument>)t.Result.Select(x => new ViewableDocument(x)).ToArray();
                    return docs;
                });
        }
    }
}
