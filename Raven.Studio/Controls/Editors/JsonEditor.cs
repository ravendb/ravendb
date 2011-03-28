namespace Raven.Studio.Controls.Editors
{
	using ActiproSoftware.Text;
	using ActiproSoftware.Windows.Controls.SyntaxEditor;

	public class JsonEditor : SyntaxEditor
    {
        private static readonly ISyntaxLanguage DefaultLanguage;

        static JsonEditor()
        {
            DefaultLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
        }

        public JsonEditor()
        {
            Document.Language = DefaultLanguage;
			IsTextDataBindingEnabled = true;
        }
    }
}