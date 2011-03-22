namespace Raven.Studio.Controls.Editors
{
	using ActiproSoftware.Text;
	using ActiproSoftware.Windows.Controls.SyntaxEditor;

	public class LinqEditor : SyntaxEditor
    {
        private static readonly ISyntaxLanguage DefaultLanguage;

        static LinqEditor()
        {
            DefaultLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("CSharp.langdef");
        }

        public LinqEditor()
        {
            Document.Language = DefaultLanguage;
			IsTextDataBindingEnabled = true; 
        }
    }
}