using System.Windows;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor;

namespace Raven.Studio.Controls.Editors
{
	public class EditorBase : SyntaxEditor
	{
        public static readonly DependencyProperty BoundDocumentProperty =
            DependencyProperty.Register("BoundDocument", typeof(IEditorDocument), typeof(EditorBase), new PropertyMetadata(default(IEditorDocument), HandlePropertyChanged));

        private static void HandlePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var editor = d as EditorBase;
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
            get { return (IEditorDocument)GetValue(BoundDocumentProperty); }
            set { SetValue(BoundDocumentProperty, value); }
        }

		public EditorBase()
		{
			IsTextDataBindingEnabled = true;
			IsLineNumberMarginVisible = false;
		}
	}
}
