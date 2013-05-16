using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Markup;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Features.Documents;
using ColumnDefinition = Raven.Studio.Features.Documents.ColumnDefinition;


namespace Raven.Studio.Behaviors
{
    public interface IDataGridColumnGenerator
    {
        DataGridColumn CreateColumnForDefinition(ColumnDefinition definition);
    }

    public abstract class DataGridColumnGenerator : DependencyObject, IDataGridColumnGenerator
    {
        public Style InvalidBindingHeaderStyle
        {
            get { return (Style)GetValue(InvalidBindingHeaderStyleProperty); }
            set { SetValue(InvalidBindingHeaderStyleProperty, value); }
        }

        public static readonly DependencyProperty InvalidBindingHeaderStyleProperty =
            DependencyProperty.Register("InvalidBindingHeaderStyle", typeof(Style), typeof(DataGridColumnGenerator), new PropertyMetadata(null));

        
        public DataGridColumn CreateColumnForDefinition(ColumnDefinition columnDefinition)
        {
            var cellTemplate = CreateCellTemplate(columnDefinition);

            DataGridColumn column;

            if (cellTemplate != null)
            {
                column = new DataGridTemplateColumn
                {
                    ClipboardContentBinding = GetBinding(columnDefinition),
                    Header = columnDefinition.Header,
                    CellTemplate = cellTemplate,
                };
            }
            else
            {
                column = new DataGridTextColumn
                {
                    Binding = new Binding("NonExistantProperty"),
                    Header = columnDefinition,
                    HeaderStyle = InvalidBindingHeaderStyle,
                };
            }

            if (!string.IsNullOrEmpty(columnDefinition.DefaultWidth))
            {
                column.Width = ParseWidth(columnDefinition.DefaultWidth);
            }

            return column;
        }

        private DataGridLength ParseWidth(string defaultWidth)
        {
            if (string.IsNullOrEmpty(defaultWidth))
                return new DataGridLength(100);

            var converter = new DataGridLengthConverter();
            return (DataGridLength)converter.ConvertFromString(defaultWidth);
        }

        private DataTemplate CreateCellTemplate(Features.Documents.ColumnDefinition columnDefinition)
        {
            var templateString = GetXamlForDataTemplate(columnDefinition);

            try
            {
                var template = XamlReader.Load(templateString) as DataTemplate;
                if (template == null)
                {
                    throw new InvalidOperationException("Xaml did not produce a DataTemplate");
                }
                template.LoadContent();
                return template;
            }
            catch (XamlParseException)
            {
                return null;
            }
        }

        protected abstract string GetXamlForDataTemplate(ColumnDefinition definition);

        protected abstract Binding GetBinding(ColumnDefinition definition);
    }
}
