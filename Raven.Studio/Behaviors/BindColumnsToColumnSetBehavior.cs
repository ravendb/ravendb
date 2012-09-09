using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Net;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Markup;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Extensions;
using Raven.Studio.Features.Documents;
using System.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Infrastructure.Converters;
using ColumnDefinition = Raven.Studio.Features.Documents.ColumnDefinition;

namespace Raven.Studio.Behaviors
{
    public class BindColumnsToColumnSetBehavior : Behavior<DataGrid>
    {
        public static readonly DependencyProperty ColumnsProperty =
            DependencyProperty.Register("Columns", typeof (ColumnsModel), typeof (BindColumnsToColumnSetBehavior), new PropertyMetadata(default(ColumnsModel), HandleColumnsModelChanged));

        private static readonly DependencyProperty AssociatedColumnProperty =
            DependencyProperty.RegisterAttached("AssociatedModel", typeof(ColumnDefinition), typeof(BindColumnsToColumnSetBehavior), new PropertyMetadata(null));

        public static readonly DependencyProperty InvalidBindingHeaderStyleProperty =
            DependencyProperty.Register("InvalidBindingHeaderStyle", typeof (Style), typeof (BindColumnsToColumnSetBehavior), new PropertyMetadata(default(Style)));

        public Style InvalidBindingHeaderStyle
        {
            get { return (Style) GetValue(InvalidBindingHeaderStyleProperty); }
            set { SetValue(InvalidBindingHeaderStyleProperty, value); }
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
            Columns.Source = ColumnsSource.User;
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
                .ToDictionary(c => GetAssociatedColumn(c), c => c.ActualWidth);
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

        private int GetIndexOfAssociatedColumn(ColumnDefinition columnModel)
        {
            var column = FindAssociatedColumn(columnModel);
            return column == null ? -1 : AssociatedObject.Columns.IndexOf(column);
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
            var cellTemplate = CreateCellTemplate(columnDefinition);

            DataGridColumn column;
            
            if (cellTemplate != null)
            {
                column = new DataGridTemplateColumn()
                             {
                                 ClipboardContentBinding = new Binding("Item.Document." + ExpandBinding(columnDefinition.Binding)) { Converter = DocumentPropertyToSingleLineStringConverter.Default },
                                 Header = columnDefinition.Header,
                                 CellTemplate = cellTemplate,
                             };
            }
            else
            {
                column = new DataGridTextColumn()
                                 {
                                     Binding = new Binding( "NonExistantProperty"),
                                     Header = columnDefinition,
                                     HeaderStyle = InvalidBindingHeaderStyle,
                                 };
            }

            if (!string.IsNullOrEmpty(columnDefinition.DefaultWidth))
            {
                column.Width = ParseWidth(columnDefinition.DefaultWidth);
            }

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
                @"<DataTemplate  xmlns=""http://schemas.microsoft.com/winfx/2006/xaml/presentation"" xmlns:Behaviors=""clr-namespace:Raven.Studio.Behaviors;assembly=Raven.Studio""
 xmlns:m=""clr-namespace:Raven.Studio.Infrastructure.MarkupExtensions;assembly=Raven.Studio""
                                    xmlns:Converters=""clr-namespace:Raven.Studio.Infrastructure.Converters;assembly=Raven.Studio"">
                                    <TextBlock Text=""{Binding Item.Document.$$$BindingPath$$$, Converter={m:Static Member=Converters:DocumentPropertyToSingleLineStringConverter.Trimmed}}""
                                               Behaviors:FadeTrimming.IsEnabled=""True"" Behaviors:FadeTrimming.ShowTextInToolTipWhenTrimmed=""True""
                                               VerticalAlignment=""Center""
                                               Margin=""5,0""/>
                                </DataTemplate>";

            templateString = templateString.Replace("$$$BindingPath$$$", ExpandBinding(columnDefinition.Binding));

            try
            {
                var template = XamlReader.Load(templateString) as DataTemplate;
                template.LoadContent();
                return template;
            }
            catch (XamlParseException)
            {
                return null;
            }
        }

        private string ExpandBinding(string binding)
        {
	        if (binding.StartsWith("$JsonDocument:"))
            {
                return binding.Substring("$JsonDocument:".Length);
            }
	        if (binding.StartsWith("$Meta:"))
	        {
		        return "Metadata" + ExpandPropertyPathToXamlBinding(binding.Substring("$Meta:".Length));
	        }
			if (binding == "$Temp:Score")
			{
				return "TempIndexScore";
			}
	        else
	        {
		        return "DataAsJson" + ExpandPropertyPathToXamlBinding(binding);
	        }
        }

	    private string ExpandPropertyPathToXamlBinding(string binding)
        {
            // for example TestNested[0].MyProperty will be expanded to [TestNested][0][MyProperty]
            var result = binding.Split(new[] {'.'}, StringSplitOptions.RemoveEmptyEntries)
                .SelectMany(s => s.Split(new[] { '[', ']' }, StringSplitOptions.RemoveEmptyEntries))
                .Aggregate(new StringBuilder(), (sb, next) =>
                                                    {
                                                        sb.Append('[');
                                                        sb.Append(next);
                                                        sb.Append(']');
                                                        return sb;
                                                    }, sb => sb.ToString());

            return result;
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
                    behavior.ScheduleColumnReset();
                }
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
