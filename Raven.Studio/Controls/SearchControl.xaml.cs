using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

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
			set {Visibility = value ? Visibility.Visible : Visibility.Collapsed;}
		}

		public string Text
		{
			get { return searchField.Text; }
			set { searchField.Text = value; }
		}

		public int NumberOfResults { get; set; }
		public int SelectedResult { get; set; }

		private void Close_Click(object sender, RoutedEventArgs e)
		{
			IsActive = false;
		}

		private void searchField_TextChanged(object sender, TextChangedEventArgs e)
		{
			//TODO: will it get the string of text to search in ?
		}
	}
}