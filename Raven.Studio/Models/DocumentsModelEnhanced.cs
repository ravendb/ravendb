using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
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
using Raven.Studio.Commands;
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
        
        /// <summary>
        /// This property is used to give bound views a wrapper around the actual VirtualCollection to prevent memory leaks where
        /// a ListBox subscribes to the ICollectionView.CurrentChanged event, and doesn't unsubscribe
        /// </summary>
        public WeakCollectionViewWrapper<VirtualCollection<ViewableDocument>> DocumentsWeak { get { return new WeakCollectionViewWrapper<VirtualCollection<ViewableDocument>>(Documents); } }
 
        private ColumnsModel columns;

        public bool SkipAutoRefresh { get; set; }
        public bool ShowEditControls { get; set; }

        private string header;
        private string context;

        private ICommand editColumns;
        private bool documentsHaveId;
        private ICommand deleteSelectedDocuments;
        private ICommand copyIdsToClipboard;
        private MostRecentUsedList<VirtualItem<ViewableDocument>> mostRecentDocuments = new MostRecentUsedList<VirtualItem<ViewableDocument>>(60);
        
        public DocumentsModelEnhanced(VirtualCollectionSource<ViewableDocument> collectionSource)
        {
            Documents = new VirtualCollection<ViewableDocument>(collectionSource, 30, 30, new KeysComparer<ViewableDocument>(v => v.Id ?? v.DisplayId, v => v.LastModified));

            Observable.FromEventPattern<ItemsRealizedEventArgs>(h => Documents.ItemsRealized += h,
                                                                h => Documents.ItemsRealized -= h)
                .SampleResponsive(TimeSpan.FromSeconds(1))
                .ObserveOnDispatcher()
                .Subscribe(e => HandleItemsRealized(e.Sender, e.EventArgs));

            ShowEditControls = true;
            ItemSelection = new ItemSelection<VirtualItem<ViewableDocument>>();

            Context = "Default";
        }

        public ItemSelection<VirtualItem<ViewableDocument>> ItemSelection { get; private set; }

        private void HandleItemsRealized(object sender, ItemsRealizedEventArgs e)
        {
            DocumentsHaveId = !string.IsNullOrEmpty(Documents[e.StartingIndex].Item.Id);

            // When a view is refreshed, items can be realized in different orders (depending on the order the query responses come back from the db)
            // So to stabilise the column set, we keep a list of 60 most recently used documents, and then sort them in index order. 
            mostRecentDocuments.AddRange(Enumerable.Range(e.StartingIndex, e.Count).Select(i => Documents[i]));

            if (Columns.Source == ColumnsSource.Automatic)
            {
                var newColumns = GetCurrentColumnsSuggestion();

                if (!Columns.Columns.Select(c => c.Binding).SequenceEqual(newColumns.Select(c => c.Binding)))
                {
                    Columns.LoadFromColumnDefinitions(newColumns);
                }
            }
        }

        private IList<ColumnDefinition> GetCurrentColumnsSuggestion()
        {
            var suggester = new ColumnSuggester();
            var newColumns =
                suggester.AutoSuggest(GetMostRecentDocuments(), Context).Select(
                    s => new ColumnDefinition() {Binding = s, Header = s}).ToList();

            return newColumns;
        }

        private IEnumerable<ViewableDocument> GetMostRecentDocuments()
        {
            return mostRecentDocuments
                .Where(i => i.IsRealized)
                .OrderBy(i => i.Index)
                .Select(i => i.Item);
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

        public bool DocumentsHaveId
        {
            get { return documentsHaveId; }
            set
            {
                if (documentsHaveId != value)
                {
                    documentsHaveId = value;
                    OnPropertyChanged(() => DocumentsHaveId);
                }
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

                TryLoadDefaultColumnSet();
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

        public ICommand DeleteSelectedDocuments
        {
            get { return deleteSelectedDocuments ?? (deleteSelectedDocuments = new DeleteDocumentsCommand(ItemSelection)); }
        }

        public ICommand CopyIdsToClipboard
        {
            get { return copyIdsToClipboard ?? (copyIdsToClipboard = new CopyDocumentsIdsCommand(ItemSelection)); }
        }

        protected override void OnViewLoaded()
        {
            UpdateColumnSet();

            ApplicationModel.Database
                .ObservePropertyChanged()
                .TakeUntil(Unloaded)
                .Subscribe(_ => UpdateColumnSet());
        }

        private void TryLoadDefaultColumnSet()
        {
            var contextWhenRequested = Context;

            ApplicationModel.DatabaseCommands
                .GetAsync("Raven/Studio/Columns/" + contextWhenRequested)
                .ContinueOnSuccessInTheUIThread(result => UpdateColumns(result, contextWhenRequested));
        }

        private void UpdateColumns(JsonDocument columnSetDocument, string contextWhenRequested)
        {
            if (contextWhenRequested != Context)
            {
                return;
            }

            if (columnSetDocument != null)
            {
                var columnSet = columnSetDocument.DataAsJson.Deserialize<ColumnSet>(new DocumentConvention() {});
                Columns.LoadFromColumnDefinitions(columnSet.Columns);
                Columns.Source = ColumnsSource.User;
            }
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
            ColumnsEditorDialog.Show(
                Columns, 
                Context, 
                () => new ColumnSuggester().AllSuggestions(GetMostRecentDocuments()),
                GetCurrentColumnsSuggestion);
        }
    }
}
