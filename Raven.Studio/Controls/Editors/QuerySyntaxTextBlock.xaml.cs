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
using ActiproSoftware.Text.Implementation;

namespace Raven.Studio.Controls.Editors
{
    public partial class QuerySyntaxTextBlock : UserControl
    {
        private static readonly ISyntaxLanguage Language;

        public static readonly DependencyProperty TextProperty =
            DependencyProperty.Register("Text", typeof (string), typeof (QuerySyntaxTextBlock), new PropertyMetadata(default(string), HandleTextChanged));

        public static readonly DependencyProperty IsMultiLineProperty =
            DependencyProperty.Register("IsMultiLine", typeof (bool), typeof (QuerySyntaxTextBlock), new PropertyMetadata(false, HandleIsMultiLineChanged));

        private static readonly SolidColorBrush FieldBrush;
        private static readonly SolidColorBrush OperatorBrush;
        private static readonly SolidColorBrush ValueBrush;

        public bool IsMultiLine
        {
            get { return (bool) GetValue(IsMultiLineProperty); }
            set { SetValue(IsMultiLineProperty, value); }
        }

        private static void HandleTextChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var tb = d as QuerySyntaxTextBlock;

            tb.UpdateText((string)(e.NewValue));
        }

        private static void HandleIsMultiLineChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var tb = d as QuerySyntaxTextBlock;

            tb.TextBlock.TextWrapping = (bool) (e.NewValue) ? TextWrapping.Wrap : TextWrapping.NoWrap;
        }

        public string Text
        {
            get { return (string) GetValue(TextProperty); }
            set { SetValue(TextProperty, value); }
        }

        static QuerySyntaxTextBlock()
        {
            Language = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef");
            FieldBrush = new SolidColorBrush(Color.FromArgb(0xff, 0x00,0x00, 0xcd));
            OperatorBrush = new SolidColorBrush(Color.FromArgb(255, 137,137, 137));
            ValueBrush = new SolidColorBrush(Colors.Black);
        }

        public QuerySyntaxTextBlock()
        {
            InitializeComponent();

            TextBlock.TextWrapping = TextWrapping.NoWrap;
        }

        private void UpdateText(string text)
        {
            // we use a RichTextBlock here instead of the SyntaxEditor because the SyntaxEditor
            // adds too much overhead when we don't need editing capabilities

            var document = new CodeDocument();
            document.Language = Language;
            document.SetText(text);
            var reader = document.CurrentSnapshot.GetReader(0);

            var paragraph = new Paragraph();

            while (!reader.IsAtSnapshotEnd)
            {
                var token = reader.PeekToken();
                var tokenText = reader.ReadText(token.Length);

                var run = new Run() {Text = tokenText};

                if (token.Key == "Field")
                {
                    run.Foreground = FieldBrush;
                }
                else if (token.Key == "Operator")
                {
                    run.Foreground = OperatorBrush;
                }
                else if (token.Key == "Value")
                {
                    run.Foreground = ValueBrush;
                }

                paragraph.Inlines.Add(run);
            }

            TextBlock.Blocks.Clear();
            TextBlock.Blocks.Add(paragraph);
        }
    }
}
