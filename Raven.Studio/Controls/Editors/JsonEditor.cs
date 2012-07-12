using System.Windows;
using System.Windows.Media;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Tagging.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using Raven.Studio.Features.JsonEditor;

namespace Raven.Studio.Controls.Editors
{
	public class JsonEditor : EditorBase
	{
		private static readonly ISyntaxLanguage DefaultLanguage;

	    public static readonly DependencyProperty BoundDocumentProperty =
            DependencyProperty.Register("BoundDocument", typeof(IEditorDocument), typeof(JsonEditor), new PropertyMetadata(default(IEditorDocument), HandlePropertyChanged));

	    private static void HandlePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
	    {
	        var editor = d as JsonEditor;
            if (e.NewValue != null)
            {
                editor.Document = e.NewValue as IEditorDocument;
            }
	    }

        /// <summary>
        /// This property exists as a work-around for a bug that appears to happen when databinding the SyntaxEditor.Document property directly
        /// </summary>
	    public IEditorDocument BoundDocument
	    {
	        get { return (IEditorDocument) GetValue(BoundDocumentProperty); }
	        set { SetValue(BoundDocumentProperty, value); }
	    }

		static JsonEditor()
		{
			DefaultLanguage = new JsonSyntaxLanguageExtended();
		}

		public JsonEditor()
		{
            IsTextDataBindingEnabled = false;
            Document.Language = DefaultLanguage;
            IsOutliningMarginVisible = true;
            Document.OutliningMode = OutliningMode.Automatic;
		}
	}
}