namespace Raven.Studio.Controls
{
	using ActiproSoftware.Text;
	using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Common;

	public class SyntaxEditor : ActiproSoftware.Windows.Controls.SyntaxEditor.SyntaxEditor
	{
		static readonly ISyntaxLanguage DefaultLanguage;

		static SyntaxEditor()
		{
			DefaultLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("CSharp.langdef");
		}

		public SyntaxEditor()
		{
			Document.Language = DefaultLanguage;
		}
	}
}