namespace Raven.Studio.Controls.Editors
{
	using ActiproSoftware.Text;

	public class LinqEditor : EditorBase
	{
		private static readonly ISyntaxLanguage DefaultLanguage;

		static LinqEditor()
		{
			DefaultLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("CSharp.langdef");
		}

		public LinqEditor()
		{
			Document.Language = DefaultLanguage;
		}
	}
}