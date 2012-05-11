using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System.Reactive.Linq;

namespace Raven.Studio.Features.Documents
{
    public class ColumnsEditorDialogViewModel : ViewModel
    {
        private readonly ColumnsModel columns;
        private readonly string context;
        private readonly Func<Task<IList<SuggestedColumn>>> suggestedColumnLoader;
        private ObservableCollection<ColumnEditorViewModel> columnEditorViewModels;
        private ICommand applyCommand;
        private ColumnEditorViewModel selectedColumn;
        private ICommand deleteSelectedColumn;
        private ICommand moveSelectedColumnUp;
        private ICommand moveSelectedColumnDown;
        private ICommand saveAsDefault;
        private ICommand addSuggestedColumn;
        private Dictionary<string, SuggestedColumn> columnsByBinding = new Dictionary<string, SuggestedColumn>();

        public ColumnsEditorDialogViewModel(ColumnsModel columns, string context, Func<Task<IList<SuggestedColumn>>> suggestedColumnLoader)
        {
            this.columns = columns;
            this.context = context;
            this.suggestedColumnLoader = suggestedColumnLoader;
            columnEditorViewModels = new ObservableCollection<ColumnEditorViewModel>(this.columns.Columns.Select(c => new ColumnEditorViewModel(c)));
            SuggestedColumns = new ObservableCollection<SuggestedColumn>();
            AddEmptyRow();
        }

        private void AddEmptyRow()
        {
            var newRow = new ColumnEditorViewModel();
            newRow.ChangesCommitted += HandleNewRowChangesCommitted;

            columnEditorViewModels.Add(newRow);
        }

        private void HandleNewRowChangesCommitted(object sender, EventArgs e)
        {
            var row = sender as ColumnEditorViewModel;
            row.ChangesCommitted -= HandleNewRowChangesCommitted;

            AddEmptyRow();
        }

        public ColumnEditorViewModel SelectedColumn
        {
            get
            {
                return selectedColumn;
            } 
            set
            {
                selectedColumn = value;
                OnPropertyChanged(() => SelectedColumn);
            }
        }

        public ICommand MoveSelectedColumnUp
        {
            get
            {
                return moveSelectedColumnUp ??
                       (moveSelectedColumnUp = new ActionCommand(() => HandleMoveSelectedColumn(-1)));
            }
        }

        public ICommand MoveSelectedColumnDown
        {
            get
            {
                return moveSelectedColumnDown ??
                       (moveSelectedColumnDown = new ActionCommand(() => HandleMoveSelectedColumn(1)));
            }
        }

        public ICommand SaveAsDefault
        {
            get { return saveAsDefault ?? (saveAsDefault = new ActionCommand(HandleSaveAsDefault)); }
        }

        public ICommand AddSuggestedColumn
        {
            get { return addSuggestedColumn ?? (addSuggestedColumn = new ActionCommand(HandleAddSuggestedColumn)); }
        }

        public ObservableCollection<SuggestedColumn> SuggestedColumns { get; private set; } 

        private void HandleAddSuggestedColumn(object parameter)
        {
            var column = parameter as SuggestedColumn;
            if (column == null)
            {
                return;
            }

            Columns.Insert(Columns.Count - 1, new ColumnEditorViewModel(column.ToColumnDefinition()));
            SuggestedColumns.Remove(column);
        }

        private void HandleSaveAsDefault()
        {
            SyncChangesWithColumnsModel();

            var columnSet = new ColumnSet() {Columns = GetCurrentColumnDefinitions()};
            var document = RavenJObject.FromObject(columnSet);

            ApplicationModel.DatabaseCommands.PutAsync("Raven/Studio/Columns/" + context, null, document,
                                                       new RavenJObject());
        }

        private void HandleMoveSelectedColumn(int change)
        {
            var columnToMove = SelectedColumn;

            if (change == 0 || columnToMove == null || columnToMove.IsNewRow)
            {
                return;
            }

            int currentIndex = Columns.IndexOf(columnToMove);
            var newIndex = currentIndex + change;

            if (newIndex < 0 || newIndex >= Columns.Count - 1)
            {
                return;
            }

            if (change < 0)
            {
                Columns.RemoveAt(currentIndex);
                Columns.Insert(newIndex, columnToMove);
            }
            else
            {
                Columns.RemoveAt(currentIndex);
                Columns.Insert(newIndex, columnToMove);
            }

            SelectedColumn = columnToMove;
        }

        public ICommand DeleteSelectedColumn
        {
            get { return deleteSelectedColumn ?? (deleteSelectedColumn = new ActionCommand(HandleDeleteSelectedColumn)); }
        }

        public ICommand Apply
        {
            get { return applyCommand ?? (applyCommand = new ActionCommand(SyncChangesWithColumnsModel)); }
        }

        public ObservableCollection<ColumnEditorViewModel> Columns
        {
            get { return columnEditorViewModels; }
        }

        private void HandleDeleteSelectedColumn()
        {
            if (SelectedColumn == null)
            {
                return;
            }

            Columns.Remove(SelectedColumn);
        }

        private void SyncChangesWithColumnsModel()
        {
            var actualColumns = GetCurrentColumnDefinitions();

            columns.Columns.Clear();

            foreach (var column in actualColumns)
            {
                columns.Columns.Add(column);
            }
        }

        protected override void OnViewLoaded()
        {
            base.OnViewLoaded();

            PopulateSuggestedColumns();
        }

        private void PopulateSuggestedColumns()
        {
            SuggestedColumns.AddRange(GetDefaultSuggestedColumns());

            suggestedColumnLoader()
                .ContinueOnSuccessInTheUIThread(result => SuggestedColumns.AddRange(result));
        }

        private IEnumerable<SuggestedColumn> GetDefaultSuggestedColumns()
        {
            return new[]
                       {
                           new SuggestedColumn() {Header = "ETag", Binding = "$JsonDocument:ETag"},
                           new SuggestedColumn() {Header = "Last Modified", Binding = "$JsonDocument:LastModified"},
                       };
        }

        private List<ColumnDefinition> GetCurrentColumnDefinitions()
        {
            return columnEditorViewModels.Where(c => !c.IsNewRow).Select(c => c.GetColumn()).ToList();
        }
    }

}
