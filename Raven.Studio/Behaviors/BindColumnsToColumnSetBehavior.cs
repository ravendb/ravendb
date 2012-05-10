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
using ColumnDefinition = Raven.Studio.Features.Documents.ColumnDefinition;

namespace Raven.Studio.Behaviors
{
    public class BindColumnsToColumnSetBehavior : Behavior<DataGrid>
    {
        public static readonly DependencyProperty ColumnsProperty =
            DependencyProperty.Register("Columns", typeof (ColumnsModel), typeof (BindColumnsToColumnSetBehavior), new PropertyMetadata(default(ColumnsModel), HandleColumnsModelChanged));

        private static readonly DependencyProperty AssociatedColumnProperty =
            DependencyProperty.RegisterAttached("AssociatedModel", typeof(ColumnDefinition), typeof(BindColumnsToColumnSetBehavior), new PropertyMetadata(null));

        private static ColumnDefinition GetAssociatedColumn(DependencyObject obj)
        {
            return (ColumnDefinition)obj.GetValue(AssociatedColumnProperty);
        }

        private static void SetAssociatedColumn(DependencyObject obj, ColumnDefinition value)
        {
            obj.SetValue(AssociatedColumnProperty, value);
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

            AssociatedObject.Loaded -= HandleLoaded;
            AssociatedObject.Unloaded -= HandleUnloaded;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            isLoaded = false;
            StopObservingColumnsModel(Columns);
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

        private void HandleColumnsChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            if (!isLoaded)
            {
                return;
            }

            if (e.Action == NotifyCollectionChangedAction.Add)
            {
                AddColumn(e.NewItems[0] as ColumnDefinition);
            }
            else if (e.Action == NotifyCollectionChangedAction.Remove)
            {
                RemoveColumn(e.OldItems[0] as ColumnDefinition);
            }
            else if (e.Action == NotifyCollectionChangedAction.Replace)
            {
                var index = GetIndexOfAssociatedColumn(e.OldItems[0] as ColumnDefinition);
                RemoveColumn(e.OldItems[0] as ColumnDefinition);
                AddColumn(e.NewItems[0] as ColumnDefinition, index);
            }
            else if (e.Action == NotifyCollectionChangedAction.Reset)
            {
                ResetColumns();
            }
            
        }

        private int GetIndexOfAssociatedColumn(ColumnDefinition columnModel)
        {
            var column = FindAssociatedColumn(columnModel);
            return column == null ? -1 : AssociatedObject.Columns.IndexOf(column);
        }

        private void ResetColumns()
        {
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
            var columnsToRemove = AssociatedObject.Columns.Where(c => GetAssociatedColumn(c) != null).ToList();

            foreach (var column in columnsToRemove)
            {
                AssociatedObject.Columns.Remove(column);
            }
        }

        private void AddColumn(ColumnDefinition columnDefinition, int? index = null)
        {
            var column = new DataGridTemplateColumn()
                             {
                                 Header = columnDefinition.Header,
                                 CellTemplate = CreateCellTemplate(columnDefinition),
                                 Width = ParseWidth(columnDefinition.DefaultWidth),
                             };

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

        private void RemoveColumn(ColumnDefinition columnDefinition)
        {
            DataGridTemplateColumn column = FindAssociatedColumn(columnDefinition);

            if (column != null)
            {
                AssociatedObject.Columns.Remove(column);
            }
        }

        private DataGridTemplateColumn FindAssociatedColumn(ColumnDefinition columnDefinition)
        {
            return AssociatedObject.Columns.FirstOrDefault(c => GetAssociatedColumn(c) == columnDefinition) as DataGridTemplateColumn;
        }

        private DataTemplate CreateCellTemplate(ColumnDefinition columnDefinition)
        {
            var templateString =
                @"<DataTemplate  xmlns=""http://schemas.microsoft.com/winfx/2006/xaml/presentation"" xmlns:Behaviors=""clr-namespace:Raven.Studio.Behaviors;assembly=Raven.Studio"">
                                    <TextBlock Text=""{Binding Item.Document.$$$BindingPath$$$}""
                                               Behaviors:FadeTrimming.IsEnabled=""True"" Behaviors:FadeTrimming.ShowTextInToolTipWhenTrimmed=""True""
                                               VerticalAlignment=""Center""
                                               Margin=""5,0""/>
                                </DataTemplate>";

            templateString = templateString.Replace("$$$BindingPath$$$", columnDefinition.Binding);

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
