using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor;

namespace Raven.Studio.Features.Patch
{
    public class AutoCompletionTrigger : IEditorDocumentTextChangeEventSink
    {
        public void NotifyDocumentTextChanged(SyntaxEditor editor, EditorSnapshotChangedEventArgs e)
        {
            if (e.TextChange.Type == TextChangeTypes.Typing && e.TypedText == ".")
                editor.ActiveView.IntelliPrompt.RequestCompletionSession();
        }

        public void NotifyDocumentTextChanging(SyntaxEditor editor, EditorSnapshotChangingEventArgs e)
        {
        }
    }
}