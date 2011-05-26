using System.Windows.Controls;
using System.Windows.Input;

namespace Raven.Studio.Controls
{
	public class WrapListBox : ListBox
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
					e.Handled = false;
					return;
					break;
				case Key.Right:
				case Key.Down:
				case Key.Left:
				case Key.Up:
					HandleRightLeft(key);
					e.Handled = true;
					break;
			}

			if (e.Handled == false)
				base.OnKeyDown(e);
		}

		private void HandleRightLeft(Key key)
		{
			if (key == Key.Right || key == Key.Down)
			{
				if (SelectedIndex < Items.Count - 1)
					SelectedIndex++;
			}
			else if (key == Key.Left || key == Key.Up)
			{
				if (SelectedIndex > 0)
					SelectedIndex--;
			}
		}
	}
}