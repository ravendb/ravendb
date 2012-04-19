using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class StealthPagingDocumentsModel : Model
    {
        private ICommand refresh;
        public VirtualCollection<ViewableDocument> Documents { get; private set; }

        public ICommand Refresh { get { return refresh ?? (refresh = new ActionCommand(() => Documents.Refresh())); } }
        
        public StealthPagingDocumentsModel()
        {
            Documents = new VirtualCollection<ViewableDocument>(new DocumentsCollectionSource(), 25);
        }
    }
}
