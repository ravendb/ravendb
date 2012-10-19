using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor;

namespace Raven.Studio.Features.Patch
{
    public class AutoCompletionTrigger : IEditorDocumentTextChangeEventSink
    {
        public void NotifyDocumentTextChanged(SyntaxEditor editor, EditorSnapshotChangedEventArgs e)
        {
            if (e.TextChange.Type == TextChangeTypes.Typing && e.TypedText == ".")
            {
                editor.ActiveView.IntelliPrompt.RequestCompletionSession();
            }
        }

        public void NotifyDocumentTextChanging(SyntaxEditor editor, EditorSnapshotChangingEventArgs e)
        {
        }
    }
}
