namespace Raven.Studio.Controls
{
	using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.QuickStart.CodeOutliningCollapsedText;

	public class JsonEditor : ActiproSoftware.Windows.Controls.SyntaxEditor.SyntaxEditor
	{
		public JsonEditor()
		{
			var language = new JavascriptSyntaxLanguage();
			Document.Language = language;
		}
	}
}