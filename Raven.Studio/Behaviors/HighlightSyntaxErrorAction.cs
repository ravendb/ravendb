using System.Windows;
using System.Windows.Interactivity;
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