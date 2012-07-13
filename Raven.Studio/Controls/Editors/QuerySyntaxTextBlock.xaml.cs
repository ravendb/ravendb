using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;

namespace Raven.Studio.Controls.Editors
{
    public partial class QuerySyntaxTextBlock : UserControl
    {
        private static readonly ISyntaxLanguage Language;

        public static readonly DependencyProperty TextProperty =
            DependencyProperty.Register("Text", typeof (string), typeof (QuerySyntaxTextBlock), new PropertyMetadata(default(string), HandleTextChanged));

        private static void HandleTextChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var tb = d as QuerySyntaxTextBlock;

            tb.Editor.Document.SetText((string) e.NewValue);
        }

        public string Text
        {
            get { return (string) GetValue(TextProperty); }
            set { SetValue(TextProperty, value); }
        }

        static QuerySyntaxTextBlock()
        {
            Language = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef");
        }

        public QuerySyntaxTextBlock()
        {
            InitializeComponent();

            Editor.Document.Language = Language;
            Editor.Document.IsReadOnly = true;
            Editor.IsHitTestVisible = false;
        }
    }
}
