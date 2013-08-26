using System.Collections.Generic;
using System.Collections.Specialized;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Markup;
using Raven.Studio.Extensions;
using Raven.Studio.Features.Documents;
using System.Linq;
using ColumnDefinition = Raven.Studio.Features.Documents.ColumnDefinition;

namespace Raven.Studio.Behaviors
{
    public class BindColumnsToColumnSetBehavior : Behavior<DataGrid>
    {
        public static readonly DependencyProperty ColumnsProperty =
            DependencyProperty.Register("Columns", typeof (ColumnsModel), typeof (BindColumnsToColumnSetBehavior), new PropertyMetadata(default(ColumnsModel), HandleColumnsModelChanged));

        private static readonly DependencyProperty AssociatedColumnProperty =
            DependencyProperty.RegisterAttached("AssociatedModel", typeof(ColumnDefinition), typeof(BindColumnsToColumnSetBehavior), new PropertyMetadata(null));
        
        public static readonly DependencyProperty ColumnGeneratorProperty =
            DependencyProperty.Register("ColumnGenerator", typeof(IDataGridColumnGenerator), typeof(BindColumnsToColumnSetBehavior), new PropertyMetadata(null));

        public IDataGridColumnGenerator ColumnGenerator
        {
            get { return (IDataGridColumnGenerator)GetValue(ColumnGeneratorProperty); }
            set { SetValue(ColumnGeneratorProperty, value); }
        }

        private static ColumnDefinition GetAssociatedColumn(DependencyObject obj)
        {
            return (ColumnDefinition)obj.GetValue(AssociatedColumnProperty);
        }

        private static void SetAssociatedColumn(DependencyObject obj, ColumnDefinition value)
        {
            obj.SetValue(AssociatedColumnProperty, value);
        }
        
        private bool isLoaded;
        private bool internalColumnUpdate;
        private Dictionary<ColumnDefinition, double> columnWidths;
        private ColumnsModel cachedColumnsModel;
        private bool _isResetPending;

        public ColumnsModel Columns
        {
            get { return (ColumnsModel) GetValue(ColumnsProperty); }
            set { SetValue(ColumnsProperty, value); }
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.Loaded += HandleLoaded;
            AssociatedObject.Unloaded += HandleUnloaded;

            ScheduleColumnReset();
        }

        private DataGridColumnHeadersPresenter GetColumnHeadersPresenter()
        {
            return AssociatedObject.GetVisualDescendants().OfType<DataGridColumnHeadersPresenter>().FirstOrDefault();
        }

        private void HandleColumnHeadersMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            // we need to ensure that the column set doesn't get changed whilst the user is interacting with one
            // of the columns
			if (Columns.Source == ColumnsSource.Automatic)
				Columns.Source = ColumnsSource.Resize;
            //Columns.Source = ColumnsSource.User;
        }

        private void HandleColumnHeadersMouseLeftButtonUp(object sender, RoutedEventArgs e)
        {
            internalColumnUpdate = true;
			

            Columns.LoadFromColumnDefinitions(GetColumnDefinitionsFromColumns());

            internalColumnUpdate = false;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            StopObservingColumnsModel();

            AssociatedObject.Loaded -= HandleLoaded;
            AssociatedObject.Unloaded -= HandleUnloaded;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            var columnHeadersPresenter = GetColumnHeadersPresenter();
            if (columnHeadersPresenter != null)
            {
                columnHeadersPresenter.RemoveHandler(UIElement.MouseLeftButtonUpEvent, new MouseButtonEventHandler(HandleColumnHeadersMouseLeftButtonUp));
                columnHeadersPresenter.RemoveHandler(UIElement.MouseLeftButtonDownEvent, new MouseButtonEventHandler(HandleColumnHeadersMouseLeftButtonDown));
            }

            isLoaded = false;
            StopObservingColumnsModel();
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            var columnHeadersPresenter = GetColumnHeadersPresenter();
            if (columnHeadersPresenter != null)
            {
                columnHeadersPresenter.AddHandler(UIElement.MouseLeftButtonUpEvent, new MouseButtonEventHandler(HandleColumnHeadersMouseLeftButtonUp), true);
                columnHeadersPresenter.AddHandler(UIElement.MouseLeftButtonDownEvent, new MouseButtonEventHandler(HandleColumnHeadersMouseLeftButtonDown), true);
            }

            isLoaded = true;
            StartObservingColumnsModel(Columns);
            ScheduleColumnReset();
        }

        private void StartObservingColumnsModel(ColumnsModel columnsModel)
        {
            if (columnsModel != null)
            {
                cachedColumnsModel = columnsModel;
                cachedColumnsModel.Columns.CollectionChanged += HandleColumnsChanged;
            }
        }

        private Dictionary<ColumnDefinition, double> GetCurrentColumnWidths()
        {
            return AssociatedObject.Columns
                .Where(c => GetAssociatedColumn(c) != null)
                .ToDictionary(GetAssociatedColumn, c => c.ActualWidth);
        }

        private void StopObservingColumnsModel()
        {
            if (cachedColumnsModel != null)
            {
                cachedColumnsModel.Columns.CollectionChanged -= HandleColumnsChanged;
                cachedColumnsModel = null;
            }
        }

        private void HandleColumnsChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            if (!isLoaded || internalColumnUpdate)
            {
                return;
            }

            ScheduleColumnReset();
        }

        private void ScheduleColumnReset()
        {
            if (!_isResetPending)
            {
                _isResetPending = true;
                Dispatcher.BeginInvoke(DoReset);
            }
        }

        private void DoReset()
        {
            ClearBoundColumns();
            AddColumns();
            CacheColumnWidths();

            _isResetPending = false; 
        }

        private void CacheColumnWidths()
        {
            columnWidths = GetCurrentColumnWidths();
        }

        private void AddColumns()
        {
            if (Columns == null || ColumnGenerator == null)
            {
                return;
            } 

            foreach (var columnModel in Columns.Columns)
            {
                AddColumn(columnModel);
            }
        }

        private void ClearBoundColumns()
        {
            var columnsToRemove = AssociatedObject.Columns.Where(c => GetAssociatedColumn(c) != null).ToList();

            foreach (var column in columnsToRemove)
            {
                AssociatedObject.Columns.Remove(column);
            }
        }

        private void AddColumn(ColumnDefinition columnDefinition, int? index = null)
        {
            var column = ColumnGenerator.CreateColumnForDefinition(columnDefinition);

            SetAssociatedColumn(column, columnDefinition);

            if (!index.HasValue)
            {
                AssociatedObject.Columns.Add(column);
            }
            else
            {
                AssociatedObject.Columns.Insert(index.Value, column);
            }
        }

        private DataGridTemplateColumn FindAssociatedColumn(ColumnDefinition columnDefinition)
        {
            return AssociatedObject.Columns.FirstOrDefault(c => GetAssociatedColumn(c) == columnDefinition) as DataGridTemplateColumn;
        }

        private static void HandleColumnsModelChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var behavior = d as BindColumnsToColumnSetBehavior;

            behavior.StopObservingColumnsModel();

            if (behavior.isLoaded)
            {
                if (e.NewValue != null)
                {
                    behavior.StartObservingColumnsModel(e.NewValue as ColumnsModel);
                }

                behavior.ScheduleColumnReset();
            }
        }

        private IList<ColumnDefinition> GetColumnDefinitionsFromColumns()
        {
            var previousColumnWidths = columnWidths ?? new Dictionary<ColumnDefinition, double>();

            var boundColumns = from column in AssociatedObject.Columns
                               let columnDefinition = GetAssociatedColumn(column)
                               where columnDefinition != null
                               let widthChanged = column.Width.IsAbsolute
                                                  && previousColumnWidths.ContainsKey(columnDefinition)
                                                  && !previousColumnWidths[columnDefinition].IsCloseTo(column.ActualWidth)
                               let displayIndex = column.DisplayIndex
                               orderby displayIndex
                               select widthChanged
                                          ? new ColumnDefinition
                                                {
                                                    Binding = columnDefinition.Binding,
                                                    Header = columnDefinition.Header,
                                                    DefaultWidth = column.ActualWidth.ToString()
                                                }
                                          : columnDefinition;

            return boundColumns.ToList();
        } 
    }
}