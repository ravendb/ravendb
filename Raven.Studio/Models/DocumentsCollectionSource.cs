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
        protected override Task<int> GetCount()
        {
            return TaskEx.FromResult((int)ApplicationModel.Database.Value.Statistics.Value.CountOfDocuments);
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
