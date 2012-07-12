using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text.Parsing;
using ActiproSoftware.Windows.Controls.SyntaxEditor;

namespace Raven.Studio.Behaviors
{
    public class HighlightSyntaxErrorAction : TargetedTriggerAction<SyntaxEditor>
    {
        public static readonly DependencyProperty ParseErrorProperty =
            DependencyProperty.Register("ParseError", typeof (IParseError), typeof (HighlightSyntaxErrorAction), new PropertyMetadata(default(IParseError)));

        public IParseError ParseError
        {
            get { return (IParseError) GetValue(ParseErrorProperty); }
            set { SetValue(ParseErrorProperty, value); }
        }

        protected override void Invoke(object parameter)
        {
            if (Target != null && ParseError != null)
            {
                Target.ActiveView.Selection.StartPosition = ParseError.PositionRange.StartPosition;
            }
        }
    }
}
