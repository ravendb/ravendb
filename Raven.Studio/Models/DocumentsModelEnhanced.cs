using System;
using System.Net;
using System.Windows;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Client.Connection;
using Raven.Studio.Extensions;
using System.Reactive.Linq;

namespace Raven.Studio.Models
{
    public class DocumentsModelEnhanced : ViewModel
    {
        private EditVirtualDocumentCommand editDocument;
        private Func<string, int, DocumentNavigator> documentNavigatorFactory;

        public VirtualCollection<ViewableDocument> Documents { get; private set; }
        
        private ColumnsModel columns;

        public bool SkipAutoRefresh { get; set; }
        public bool ShowEditControls { get; set; }

        private string header;
        private string context;

        private ICommand editColumns;

        public DocumentsModelEnhanced(VirtualCollectionSource<ViewableDocument> collectionSource)
        {
            Documents = new VirtualCollection<ViewableDocument>(collectionSource, 25, 30, new KeysComparer<ViewableDocument>(v => v.Id ?? v.DisplayId, v => v.LastModified));

            ShowEditControls = true;

            Context = "Default";
        }

        public string Context
        {
            get { return context; }
            set
            {
                context = value ?? "Default";
                UpdateColumnSet();
            }
        }

        private void UpdateColumnSet()
        {
            if (!IsLoaded)
            {
                return;
            }

            var columnsModel = PerDatabaseState.DocumentViewState.GetDocumentState(context);

            if (columnsModel != null)
            {
                Columns = columnsModel;
            }
            else
            {
                Columns = new ColumnsModel();
                PerDatabaseState.DocumentViewState.SetDocumentState(context, Columns);

                BeginLoadColumnSet();
            }
        }

        public ColumnsModel Columns
        {
            get { return columns; }
            private set
            {
                columns = value;
                OnPropertyChanged(() => Columns);
            }
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

        public ICommand EditColumns
        {
            get { return editColumns ?? (editColumns = new ActionCommand(HandleEditColumns)); }
        }

        protected override void OnViewLoaded()
        {
            UpdateColumnSet();

            ApplicationModel.Database
                .ObservePropertyChanged()
                .TakeUntil(Unloaded)
                .Subscribe(_ => UpdateColumnSet());
        }

        private void BeginLoadColumnSet()
        {
            ApplicationModel.DatabaseCommands
                .GetAsync("Raven/Studio/Columns/" + Context)
                .ContinueOnSuccessInTheUIThread(UpdateColumns);
        }

        private void UpdateColumns(JsonDocument columnSetDocument)
        {
            var columnSet = columnSetDocument == null
                                ? GetDefaultColumnSet()
                                : columnSetDocument.DataAsJson.Deserialize<ColumnSet>(new DocumentConvention() {});

            Columns.LoadFromColumnDefinitions(columnSet.Columns);
        }

        private ColumnSet GetDefaultColumnSet()
        {
            return new ColumnSet()
                       {
                           Columns =
                               {
                                   new ColumnDefinition() {Header = "Id", Binding = "$meta:@id"},
                                   new ColumnDefinition() {Header = "Last Modified", Binding = "$meta:Last-Modified"},
                               }
                       };
        }


        public override System.Threading.Tasks.Task TimerTickedAsync()
        {
            if (SkipAutoRefresh)
            {
                return null;
            }

            Documents.Refresh();
            return base.TimerTickedAsync();
        }

        public string Header
        {
            get { return header ?? (header = "Documents"); }
            set
            {
                header = value;
                OnPropertyChanged(() => Header);
            }
        }

        private void HandleEditColumns()
        {
            ColumnsEditorDialog.Show(Columns, Context);
        }
    }
}
