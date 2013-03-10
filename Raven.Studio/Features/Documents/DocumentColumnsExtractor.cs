using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;

namespace Raven.Studio.Features.Documents
{
    /// <summary>
    /// Given a list of ColumnDefinitions, will extract the values specified by the binding of each column definition from a ViewableDocument
    /// </summary>
    /// <remarks>
    /// Must be called on the UI Thread, because it uses FrameworkObjects to do the value extraction
    /// </remarks>
    internal class DocumentColumnsExtractor
    {
        private List<BoundValueExtractor> valueExtractors;

        public DocumentColumnsExtractor(IList<ColumnDefinition> columns)
        {
            valueExtractors = columns.Select(c =>
            {
                var extractor = new BoundValueExtractor();
                extractor.SetBinding(BoundValueExtractor.ValueProperty, ColumnModelBindingExtensions.CreateBinding(c, "Document."));
                return extractor;
            }).ToList();
        }

        public IList<object> GetValues(ViewableDocument document)
        {
            var values = valueExtractors.Select(e =>
            {
                e.DataContext = document;
                return e.Value;
            }).ToArray();

            return values;
        } 

        private class BoundValueExtractor : FrameworkElement
        {
            public static readonly DependencyProperty ValueProperty =
                DependencyProperty.Register("Value", typeof (object), typeof (BoundValueExtractor),
                                            new PropertyMetadata(default(Type)));

            public object Value
            {
                get { return (object)GetValue(ValueProperty); }
                set { SetValue(ValueProperty, value); }
            }
        }
    }
}