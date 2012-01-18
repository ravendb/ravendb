using System.Windows;
using System.Windows.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Views
{
	public partial class Edit : View
	{
		private bool isCtrlHold;
		
		public Edit()
		{
			InitializeComponent();

			KeyDown += OnKeyDown;
			KeyUp += OnKeyUp;
		}

		private void OnKeyUp(object sender, KeyEventArgs args)
		{
			switch (args.Key)
			{
				case Key.Ctrl:
					isCtrlHold = false;
					break;
			}
		}

		private void OnKeyDown(object sender, KeyEventArgs args)
		{
			switch (args.Key)
			{
				case Key.F:
					if (isCtrlHold)
					{
						EnableSearch();
					}
					break;
				case Key.Escape:
					if (SearchTool.Visibility == Visibility.Visible)
					{
						DisableSearch();
					}
					break;
				case Key.Ctrl:
					isCtrlHold = true;
					break;
			}
		}

		private void DisableSearch()
		{
			SearchTool.IsActive = false;
		}

		private void EnableSearch()
		{
			SearchTool.IsActive = true;
		}

		private void Search_Click(object sender, RoutedEventArgs e)
		{
			if(SearchTool.Visibility == Visibility.Visible)
				DisableSearch();
			else
			{
				EnableSearch();
			}
		}
	}
}