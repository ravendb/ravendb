using ActiproSoftware.Windows.Controls.SyntaxEditor;

namespace Raven.Studio.Controls.Editors
{
	public class EditorBase : SyntaxEditor
	{
		public EditorBase()
		{
			IsTextDataBindingEnabled = true;
			IsLineNumberMarginVisible = false;
		}
	}
}