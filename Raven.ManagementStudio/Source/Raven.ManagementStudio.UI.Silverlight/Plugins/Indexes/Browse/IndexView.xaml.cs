using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Common;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
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
