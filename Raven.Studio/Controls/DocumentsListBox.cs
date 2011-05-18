using System.Windows.Controls;
using System.Windows.Input;

namespace Raven.Studio.Controls
{
	public class DocumentsListBox : ListBox
	{
		protected override void OnKeyDown(KeyEventArgs e)
		{
			if (e.Handled)
				return;

			var key = e.Key;
			switch (key)
			{
				case Key.Delete:
					break;
				case Key.PageUp:
				case Key.PageDown:
					break;
				case Key.Right:
				case Key.Left:
					HandleRightLeft(key);
					e.Handled = true;
					break;
			}

			if (e.Handled == false)
				base.OnKeyDown(e);
		}

		private void HandleRightLeft(Key key)
		{
			if (key == Key.Right)
			{
				if (SelectedIndex < Items.Count - 1)
					SelectedIndex++;
			}
			else if (key == Key.Left)
			{
				if (SelectedIndex > 0)
					SelectedIndex--;
			}
		}
	}
}