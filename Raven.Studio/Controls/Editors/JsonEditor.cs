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