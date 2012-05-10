using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Markup;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Features.Documents;
using System.Linq;

namespace Raven.Studio.Behaviors
{
    public class BindColumnsToColumnSetBehavior : Behavior<DataGrid>
    {
        public static readonly DependencyProperty ColumnsProperty =
            DependencyProperty.Register("Columns", typeof (ColumnsModel), typeof (BindColumnsToColumnSetBehavior), new PropertyMetadata(default(ColumnsModel), HandleColumnsModelChanged));

        public static readonly DependencyProperty AssociatedModelProperty =
            DependencyProperty.RegisterAttached("AssociatedModel", typeof(ColumnModel), typeof(BindColumnsToColumnSetBehavior), new PropertyMetadata(null));

        private Dictionary<ColumnModel, DataGridTemplateColumn> _associatedColumns = new Dictionary<ColumnModel, DataGridTemplateColumn>();

        private static ColumnModel GetAssociatedModel(DependencyObject obj)
        {
            return (ColumnModel)obj.GetValue(AssociatedModelProperty);
        }

        private static void SetAssociatedModel(DependencyObject obj, ColumnModel value)
        {
            obj.SetValue(AssociatedModelProperty, value);
        }
        
        private bool isLoaded;

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

            ResetColumns();
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            StopObservingColumnsModel(Columns);
            DisassociateFromColumns();

            AssociatedObject.Loaded -= HandleLoaded;
            AssociatedObject.Unloaded -= HandleUnloaded;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            isLoaded = false;
            StopObservingColumnsModel(Columns);
            DisassociateFromColumns();
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            isLoaded = true;
            StartObservingColumnsModel(Columns);
            ResetColumns();
        }

        private void StartObservingColumnsModel(ColumnsModel columnsModel)
        {
            if (columnsModel != null)
            {
                columnsModel.Columns.CollectionChanged += HandleColumnsChanged;
            }
        }

        private void StopObservingColumnsModel(ColumnsModel columnsModel)
        {
            if (columnsModel != null)
            {
                columnsModel.Columns.CollectionChanged -= HandleColumnsChanged;
            }
        }

        private void DisassociateFromColumns()
        {
            foreach (var columnModel in _associatedColumns.Keys)
            {
                columnModel.PropertyChanged -= HandleColumnModelPropertyChanged;
            }

            _associatedColumns.Clear();
        }

        private void HandleColumnModelPropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            var columnModel = sender as ColumnModel;

            DataGridTemplateColumn column;
            if (_associatedColumns.TryGetValue(columnModel, out column))
            {
                // special-case Binding, because that is the most expensive to update
                if (e.PropertyName == "Binding")
                {
                    var index = GetIndexOfAssociatedColumn(columnModel);
                    RemoveColumn(columnModel);
                    AddColumn(columnModel, index);
                }
                else
                {
                    column.Header = columnModel.Header;
                    column.Width = ParseWidth(columnModel.DefaultWidth);
                }
            }
        }

        private void HandleColumnsChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            if (!isLoaded)
            {
                return;
            }

            if (e.Action == NotifyCollectionChangedAction.Add)
            {
                AddColumn(e.NewItems[0] as ColumnModel);
            }
            else if (e.Action == NotifyCollectionChangedAction.Remove)
            {
                RemoveColumn(e.OldItems[0] as ColumnModel);
            }
            else if (e.Action == NotifyCollectionChangedAction.Replace)
            {
                var index = GetIndexOfAssociatedColumn(e.OldItems[0] as ColumnModel);
                RemoveColumn(e.OldItems[0] as ColumnModel);
                AddColumn(e.NewItems[0] as ColumnModel, index);
            }
            else if (e.Action == NotifyCollectionChangedAction.Reset)
            {
                ResetColumns();
            }
            
        }

        private int GetIndexOfAssociatedColumn(ColumnModel columnModel)
        {
            var column = FindAssociatedColumn(columnModel);
            return column == null ? -1 : AssociatedObject.Columns.IndexOf(column);
        }

        private void ResetColumns()
        {
            DisassociateFromColumns();
            ClearBoundColumns();
            AddColumns();
        }

        private void AddColumns()
        {
            if (Columns == null)
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
            var columnsToRemove = AssociatedObject.Columns.Where(c => GetAssociatedModel(c) != null).ToList();

            foreach (var column in columnsToRemove)
            {
                AssociatedObject.Columns.Remove(column);
            }
        }

        private void AddColumn(ColumnModel columnModel, int? index = null)
        {
            var column = new DataGridTemplateColumn()
                             {
                                 Header = columnModel.Header,
                                 CellTemplate = CreateCellTemplate(columnModel),
                                 Width = ParseWidth(columnModel.DefaultWidth),
                             };

            SetAssociatedModel(column, columnModel);
            _associatedColumns.Add(columnModel, column);

            columnModel.PropertyChanged += HandleColumnModelPropertyChanged;

            if (!index.HasValue)
            {
                AssociatedObject.Columns.Add(column);
            }
            else
            {
                AssociatedObject.Columns.Insert(index.Value, column);
            }
        }

        private DataGridLength ParseWidth(string defaultWidth)
        {
            if (string.IsNullOrEmpty(defaultWidth))
            {
                return new DataGridLength(100);
            }
            else
            {
                var converter = new DataGridLengthConverter();

                return (DataGridLength)converter.ConvertFromString(defaultWidth);
            }
        }

        private void RemoveColumn(ColumnModel columnModel)
        {
            DataGridTemplateColumn column = FindAssociatedColumn(columnModel);

            if (column != null)
            {
                AssociatedObject.Columns.Remove(column);
            }

            _associatedColumns.Remove(columnModel);

            columnModel.PropertyChanged -= HandleColumnModelPropertyChanged;
        }

        private DataGridTemplateColumn FindAssociatedColumn(ColumnModel columnModel)
        {
             DataGridTemplateColumn column;

            return _associatedColumns.TryGetValue(columnModel, out column) ? column : null;
        }

        private DataTemplate CreateCellTemplate(ColumnModel columnModel)
        {
            var templateString =
                @"<DataTemplate  xmlns=""http://schemas.microsoft.com/winfx/2006/xaml/presentation"" xmlns:Behaviors=""clr-namespace:Raven.Studio.Behaviors;assembly=Raven.Studio"">
                                    <TextBlock Text=""{Binding Item.Document.$$$BindingPath$$$}""
                                               Behaviors:FadeTrimming.IsEnabled=""True"" Behaviors:FadeTrimming.ShowTextInToolTipWhenTrimmed=""True""
                                               VerticalAlignment=""Center""
                                               Margin=""5,0""/>
                                </DataTemplate>";

            templateString = templateString.Replace("$$$BindingPath$$$", columnModel.Binding);

            var template = XamlReader.Load(templateString) as DataTemplate;

            return template;
        }

        private static void HandleColumnsModelChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var behavior = d as BindColumnsToColumnSetBehavior;

            if (e.OldValue != null)
            {
                behavior.StopObservingColumnsModel(e.OldValue as ColumnsModel);
            }

            if (behavior.isLoaded)
            {
                if (e.NewValue != null)
                {
                    behavior.StartObservingColumnsModel(e.NewValue as ColumnsModel);
                }
            }
        }
    }
}
