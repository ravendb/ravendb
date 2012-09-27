using System;
using System.Collections.Generic;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public partial class ColumnsEditorDialog : DialogView
    {
        public ColumnsEditorDialog()
        {
            InitializeComponent();

        }

        public static void Show(ColumnsModel columns, string context, Func<IList<string>> bindingSuggestions, Func<IList<ColumnDefinition>> automaticColumnsFetcher)
        {
            var dialog = new ColumnsEditorDialog
                             {
                                 DataContext = new ColumnsEditorDialogViewModel(columns, context, bindingSuggestions, automaticColumnsFetcher)
                             };
            dialog.Show();
        }
    }
}