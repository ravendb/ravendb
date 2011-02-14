namespace Raven.Studio.Features.Documents
{
	using System.Windows.Controls;
	using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.QuickStart.CodeOutliningCollapsedText;

	public partial class DocumentView : UserControl
	{
		public DocumentView()
		{
			InitializeComponent();

			var language = new JavascriptSyntaxLanguage();
			dataEditor.Document.Language = language;
			metadataEditor.Document.Language = language;
		}
	}
}