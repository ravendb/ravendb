using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
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
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
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
