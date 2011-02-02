namespace Raven.Studio.Indexes.Browse
{
	using ActiproSoftware.Text;
	using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Common;

	public partial class EditIndexView
	{
		public EditIndexView()
		{
			InitializeComponent();

			ISyntaxLanguage language = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("CSharp.langdef");
			mapEditor.Document.Language = language;
			reduceEditor.Document.Language = language;
			transformEditor.Document.Language = language;
		}
	}
}