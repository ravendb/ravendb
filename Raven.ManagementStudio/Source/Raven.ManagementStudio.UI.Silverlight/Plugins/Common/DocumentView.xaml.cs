using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.QuickStart.CodeOutliningCollapsedText;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Common
{
    using System.Windows;
    using System.Windows.Controls;

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
