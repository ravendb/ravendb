using System.Windows.Media;
using ActiproSoftware.Text;

namespace Raven.Studio.Controls.Editors
{
	public class JsonEditor : EditorBase
	{
		private static readonly ISyntaxLanguage DefaultLanguage;

		static JsonEditor()
		{
			DefaultLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
		}

		public JsonEditor()
		{
			Document.Language = DefaultLanguage;
			this.SelectionBackgroundInactive = new SolidColorBrush(Colors.Yellow);
		}
	}
}