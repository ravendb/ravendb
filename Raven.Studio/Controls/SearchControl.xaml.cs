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

		public static readonly DependencyProperty IsActiveProperty =
			DependencyProperty.Register("IsActive", typeof (bool), typeof (SearchControl), new PropertyMetadata(default(bool), IsActiveCallback));

		private static void IsActiveCallback(DependencyObject dependencyObject, DependencyPropertyChangedEventArgs args)
		{
			var searchTool = (SearchControl) dependencyObject;
			if(searchTool == null)
				return;

			if((bool)args.NewValue)
			{
				searchTool.Visibility = Visibility.Visible;
				searchTool.Text = WordHighlightTagger.SearchBeforeClose;
				searchTool.searchField.Focus();
			}
			else
			{
				searchTool.Visibility = Visibility.Collapsed;
				WordHighlightTagger.SearchBeforeClose = searchTool.Text;
				searchTool.Text = "";
			}
		}

		public bool IsActive
		{
			get { return (bool) GetValue(IsActiveProperty); }
			set { SetValue(IsActiveProperty, value); }
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