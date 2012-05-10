using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public class ColumnsEditorDialogViewModel : ViewModel
    {
        private readonly ColumnsModel columns;
        private ObservableCollection<ColumnEditorViewModel> columnEditorViewModels;
        private ICommand applyCommand;
        private ColumnEditorViewModel selectedColumn;
        private ICommand deleteSelectedColumn;
        private ICommand moveSelectedColumnUp;
        private ICommand moveSelectedColumnDown;

        public ColumnsEditorDialogViewModel(ColumnsModel columns)
        {
            this.columns = columns;
            columnEditorViewModels = new ObservableCollection<ColumnEditorViewModel>(this.columns.Columns.Select(c => new ColumnEditorViewModel(c)));
            AddEmptyRow();
        }

        private void AddEmptyRow()
        {
            var newRow = new ColumnEditorViewModel(new ColumnModel());
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
            var actualColumns = columnEditorViewModels.Where(c => !c.IsNewRow).ToList();

            foreach (var columnEditorViewModel in actualColumns)
            {
                columnEditorViewModel.ApplyChanges();
            }

            for (int i = 0; i < Math.Max(actualColumns.Count, columns.Columns.Count); i++)
            {
                if (actualColumns.Count > i && columns.Columns.Count > i)
                {
                    if (columns.Columns[i] != actualColumns[i].Column)
                    {
                        columns.Columns[i] = actualColumns[i].Column;
                    }
                }
                else if (actualColumns.Count <= i)
                {
                    columns.Columns.RemoveAt(i);
                }
                else if (columns.Columns.Count <= i)
                {
                    columns.Columns.Add(actualColumns[i].Column);
                }
            }
        }
    }

}
