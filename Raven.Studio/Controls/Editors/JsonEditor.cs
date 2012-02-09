using System.Windows.Media;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Text.Tagging.Implementation;

namespace Raven.Studio.Controls.Editors
{
	public class JsonEditor : EditorBase
	{
		private static readonly ISyntaxLanguage DefaultLanguage;

		static JsonEditor()
		{
			DefaultLanguage = new CustomSyntaxLanguage();
		}

		public JsonEditor()
		{
			Document.Language = DefaultLanguage;
			this.SelectionBackgroundInactive = new SolidColorBrush(Colors.Yellow);
		}
	}

	public class CustomSyntaxLanguage : SyntaxLanguage
	{
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// OBJECT
		/////////////////////////////////////////////////////////////////////////////////////////////////////

		/// <summary>
		/// Initializes a new instance of the <c>CustomSyntaxLanguage</c> class.
		/// </summary>
		public CustomSyntaxLanguage()
			: base("CustomDecorator")
		{
			// Initialize this language from a language definition
			SyntaxEditorHelper.InitializeLanguageFromResourceStream(this, "JScript.langdef");

			// Register a tagger provider on the language as a service that can create CustomTag objects
			this.RegisterService(new TextViewTaggerProvider<WordHighlightTagger>(typeof(WordHighlightTagger)));
		}
	}
}