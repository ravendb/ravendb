using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class ColumnsEditorDialogViewModel : ViewModel
    {
        private readonly ColumnsModel columns;
        private readonly string context;
        private ObservableCollection<ColumnEditorViewModel> columnEditorViewModels;
        private ICommand applyCommand;
        private ColumnEditorViewModel selectedColumn;
        private ICommand deleteSelectedColumn;
        private ICommand moveSelectedColumnUp;
        private ICommand moveSelectedColumnDown;
        private ICommand saveAsDefault;

        public ColumnsEditorDialogViewModel(ColumnsModel columns, string context)
        {
            this.columns = columns;
            this.context = context;
            columnEditorViewModels = new ObservableCollection<ColumnEditorViewModel>(this.columns.Columns.Select(c => new ColumnEditorViewModel(c)));
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

        private List<ColumnDefinition> GetCurrentColumnDefinitions()
        {
            return columnEditorViewModels.Where(c => !c.IsNewRow).Select(c => c.GetColumn()).ToList();
        }
    }

}
