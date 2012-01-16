using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Controls;

namespace Raven.Studio
{
	public partial class SearchControl : UserControl
	{
		public SearchControl()
		{
			// Required to initialize variables
			InitializeComponent();
		}

		public bool IsActive
		{
			get { return this.Visibility == Visibility.Visible; }
			set
			{
				if (value)
				{
					Visibility = Visibility.Visible;
					Text = WordHighlightTagger.SearchBeforeClose;
					searchField.Focus();
				}
				else
				{
					Visibility = Visibility.Collapsed;
					WordHighlightTagger.SearchBeforeClose = Text;
					Text = "";
				}
			}
		}

		public string Text
		{
			get { return searchField.Text; }
			set { searchField.Text = value; }
		}

		private void Close_Click(object sender, RoutedEventArgs e)
		{
			IsActive = false;
		}
	}
}