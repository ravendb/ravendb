namespace Raven.Studio.Controls
{
	using ActiproSoftware.Text;
	using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Common;

	public class LinqEditor : ActiproSoftware.Windows.Controls.SyntaxEditor.SyntaxEditor
	{
		static readonly ISyntaxLanguage DefaultLanguage;

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