using ActiproSoftware.Windows.Controls.SyntaxEditor;

namespace Raven.Studio.Controls.Editors
{
	public class EditorBase : SyntaxEditor
	{
		static EditorBase()
		{
			SettingsRegister.Register();
		}

		public EditorBase()
		{
			IsTextDataBindingEnabled = true;
			IsLineNumberMarginVisible = false;
		}
	}
}