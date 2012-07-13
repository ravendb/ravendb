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
		public QueryEditor()
		{
		    IsSelectionMarginVisible = false;
		    IsOutliningMarginVisible = false;
			AreLineModificationMarksVisible = false;

			foreach (var key in InputBindings.Where(x => x.Key == Key.Enter && x.Modifiers == ModifierKeys.Control).ToList())
			{
				InputBindings.Remove(key);
			}
		}
	}
}
