using ActiproSoftware.Text;

namespace Raven.Studio.Controls.Editors
{
	public class XamlEditor : EditorBase
	{
		private static readonly ISyntaxLanguage DefaultLanguage;

		static XamlEditor()
		{
			DefaultLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("Xaml.langdef");
		}

		public XamlEditor()
		{
			Document.Language = DefaultLanguage;
		}
	}
}