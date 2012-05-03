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
    public class DocumentsModelEnhanced : Model
    {
        private EditVirtualDocumentCommand editDocument;
        private Func<string, int, DocumentNavigator> documentNavigatorFactory;

        public VirtualCollection<ViewableDocument> Documents { get; private set; }

        public bool SkipAutoRefresh { get; set; }
        public bool ShowEditControls { get; set; }

        public DocumentsModelEnhanced(VirtualCollectionSource<ViewableDocument> collectionSource)
        {
            Documents = new VirtualCollection<ViewableDocument>(collectionSource, 25, 30, new KeysComparer<ViewableDocument>(v => v.Id ?? v.DisplayId, v => v.LastModified));

            ShowEditControls = true;
        }

        public Func<string, int, DocumentNavigator> DocumentNavigatorFactory
        {
            get { return documentNavigatorFactory; }
            set
            {
                documentNavigatorFactory = value;
                if (editDocument != null)
                {
                    editDocument.DocumentNavigatorFactory = value;
                }
            }
        }

        public ICommand EditDocument { get
        {
            return editDocument ??
                   (editDocument =
                    new EditVirtualDocumentCommand() {DocumentNavigatorFactory = DocumentNavigatorFactory});
        } }

        public override System.Threading.Tasks.Task TimerTickedAsync()
        {
            if (SkipAutoRefresh)
            {
                return null;
            }

            Documents.Refresh();
            return base.TimerTickedAsync();
        }

        private string header;

        public string Header
        {
            get { return header ?? (header = "Documents"); }
            set
            {
                header = value;
                OnPropertyChanged(() => Header);
            }
        }
    }
}
