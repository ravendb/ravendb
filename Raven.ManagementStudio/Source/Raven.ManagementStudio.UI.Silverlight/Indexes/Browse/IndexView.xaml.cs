namespace Raven.ManagementStudio.UI.Silverlight.Indexes.Browse
{
	using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Common;

	public partial class IndexView
    {
        public IndexView()
        {
            InitializeComponent();

            var language = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("CSharp.langdef");
            mapEditor.Document.Language = language;
            reduceEditor.Document.Language = language;
            transformEditor.Document.Language = language;
        }
    }
}
