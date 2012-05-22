using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Raven.Studio.Features.Query;

namespace Raven.Studio.Controls.Editors
{
	public class QueryEditor : EditorBase
	{
		private static readonly ISyntaxLanguage DefaultLanguage;

		static QueryEditor()
		{
			DefaultLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef");
		}

		public QueryEditor()
		{
			Document.Language = DefaultLanguage;
			AreLineModificationMarksVisible = false;

			foreach (var key in InputBindings.Where(x => x.Key == Key.Enter && x.Modifiers == ModifierKeys.Control).ToList())
			{
				InputBindings.Remove(key);
			}
		}

		public ICompletionProvider CompletionProvider
		{
			get { return (ICompletionProvider) GetValue(CompletionProviderProperty); }
			set { SetValue(CompletionProviderProperty, value); }
		}

		public static readonly DependencyProperty CompletionProviderProperty =
			DependencyProperty.Register("CompletionProvider", typeof(ICompletionProvider), typeof(QueryEditor), new PropertyMetadata(null, PropertyChangedCallback));

		private static void PropertyChangedCallback(DependencyObject dependencyObject, DependencyPropertyChangedEventArgs args)
		{
			var editor = (QueryEditor)dependencyObject;
			editor.Document.Language.RegisterService((ICompletionProvider) args.NewValue);
		}

		public static IEnumerable<FieldAndTerm> GetCurrentFieldsAndTerms(string text)
		{
			var editor = new QueryEditor {Text = text};
			var textSnapshotReader = editor.ActiveView.GetReader();
			string currentField = null;
			while (!textSnapshotReader.IsAtSnapshotEnd)
			{
				var token = textSnapshotReader.ReadToken();
				if (token == null)
					break;

				var txt = textSnapshotReader.ReadTextReverse(token.Length);
				textSnapshotReader.ReadToken();
				if(string.IsNullOrWhiteSpace(txt))
					continue;

				string currentVal = null;
				if (txt.EndsWith(":"))
				{
					currentField = txt.Substring(0, txt.Length - 1);
				}
				else 
				{
					currentVal = txt;
				}
				if (currentField == null || currentVal == null) 
					continue;

				yield return new FieldAndTerm(currentField, currentVal);
				currentField = null;
			}
		}
	}
}