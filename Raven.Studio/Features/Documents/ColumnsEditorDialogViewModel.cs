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
    public class ColumnsEditorDialogViewModel : DialogViewModel
    {
        private readonly ColumnsModel columns;
        private readonly string context;
        private readonly Func<IList<string>> suggestedBindingLoader;
        private readonly Func<IList<ColumnDefinition>> automaticColumnsFetcher;
        private ObservableCollection<ColumnEditorViewModel> columnEditorViewModels;
        private ICommand applyCommand;
        private ColumnEditorViewModel selectedColumn;
        private ICommand deleteSelectedColumn;
        private ICommand moveSelectedColumnUp;
        private ICommand moveSelectedColumnDown;
        private ICommand saveAsDefault;
        private ICommand okCommand;
        private ICommand cancelCommand;
        private bool isCustomChoice;

        public ColumnsEditorDialogViewModel(ColumnsModel columns, string context, Func<IList<string>> suggestedBindingLoader, Func<IList<ColumnDefinition>> automaticColumnsFetcher)
        {
            this.columns = columns;
            this.context = context;
            this.suggestedBindingLoader = suggestedBindingLoader;
            this.automaticColumnsFetcher = automaticColumnsFetcher;

            columnEditorViewModels = new ObservableCollection<ColumnEditorViewModel>(ColumnEditorViewModelsFromColumns(this.columns.Columns));
            AddEmptyRow();

            SuggestedBindings = new ObservableCollection<string>();

            IsCustomChoice = columns.Source == ColumnsSource.User;
        }

        private IEnumerable<ColumnEditorViewModel> ColumnEditorViewModelsFromColumns(IEnumerable<ColumnDefinition> observableCollection)
        {
            return observableCollection.Select(c => new ColumnEditorViewModel(c));
        }

        private void AddEmptyRow()
        {
            var newRow = new ColumnEditorViewModel();
            newRow.PropertyChanged += HandleNewRowPropertyChanged;

            columnEditorViewModels.Add(newRow);
        }

        private void HandleNewRowPropertyChanged(object sender, EventArgs e)
        {
            var row = sender as ColumnEditorViewModel;
            if (!row.IsNewRow)
            {
                row.PropertyChanged -= HandleNewRowPropertyChanged;
                AddEmptyRow();
            }
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

        public bool IsCustomChoice
        {
            get { return isCustomChoice; }
            set
            {
                isCustomChoice = value;
                if (!isCustomChoice)
                {
                    RepopuplateColumnsFromAutomaticChoice();
                }
                OnPropertyChanged(() => IsCustomChoice);
            }
        }

        private void RepopuplateColumnsFromAutomaticChoice()
        {
            columnEditorViewModels.Clear();
            columnEditorViewModels.AddRange(ColumnEditorViewModelsFromColumns(automaticColumnsFetcher()));
            AddEmptyRow();
        }

        public ICommand SaveAsDefault
        {
            get { return saveAsDefault ?? (saveAsDefault = new ActionCommand(HandleSaveAsDefault)); }
        }

        public ICommand OK
        {
            get { return okCommand ?? (okCommand = new ActionCommand(HandleOKCommand)); }
        }

        public ICommand Cancel
        {
            get { return cancelCommand ?? (cancelCommand = new ActionCommand(() => Close(false))); }
        }

        public ObservableCollection<string> SuggestedBindings { get; private set; }
 
        private void HandleOKCommand()
        {
            if (SyncChangesWithColumnsModel())
            {
                Close(true);
            }
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
            get { return applyCommand ?? (applyCommand = new ActionCommand(() => SyncChangesWithColumnsModel())); }
        }

        public ObservableCollection<ColumnEditorViewModel> Columns
        {
            get { return columnEditorViewModels; }
        }

        private void HandleDeleteSelectedColumn()
        {
            if (SelectedColumn == null || SelectedColumn.IsNewRow) 
            {
                return;
            }

            Columns.Remove(SelectedColumn);
        }

        private bool SyncChangesWithColumnsModel()
        {
            if (Columns.Any(c => c.HasErrors))
            {
                return false;
            }

            var actualColumns = GetCurrentColumnDefinitions();

            columns.LoadFromColumnDefinitions(actualColumns);
            columns.Source = IsCustomChoice ? ColumnsSource.User : ColumnsSource.Automatic;

            return true;
        }

        protected override void OnViewLoaded()
        {
            base.OnViewLoaded();

            PopulateSuggestedColumns();
        }

        private void PopulateSuggestedColumns()
        {
            Task.Factory.StartNew(suggestedBindingLoader)
                .ContinueOnSuccessInTheUIThread(UpdateSuggestedColumns);
        }

        private void UpdateSuggestedColumns(IList<string> suggestedColumns)
        {
            SuggestedBindings.AddRange(suggestedColumns);
        }

        private List<ColumnDefinition> GetCurrentColumnDefinitions()
        {
            return columnEditorViewModels.Where(c => !c.IsNewRow).Select(c => c.GetColumn()).ToList();
        }
    }

}
