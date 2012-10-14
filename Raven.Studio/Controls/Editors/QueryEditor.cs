using System.Linq;
using System.Windows.Input;

namespace Raven.Studio.Controls.Editors
{
	public class QueryEditor : EditorBase
    {
		public QueryEditor()
		{
		    IsSelectionMarginVisible = false;
		    IsOutliningMarginVisible = true;
			AreLineModificationMarksVisible = false;

			foreach (var key in InputBindings.Where(x => x.Key == Key.Enter && x.Modifiers == ModifierKeys.Control).ToList())
			{
				InputBindings.Remove(key);
			}
		}
	}
}