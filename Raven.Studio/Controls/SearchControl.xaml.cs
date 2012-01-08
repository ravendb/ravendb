using System;
using System.Collections.Generic;
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
			set
			{
				Visibility = value ? Visibility.Visible : Visibility.Collapsed;
				if (value)
					searchField.Focus();
			}
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

		public List<int> SearchReasoltsLocations { get; set; }

		public string SearchIn
		{
			get { return (string)GetValue(SearchInProperty); }
			set { SetValue(SearchInProperty, value); }
		}

		public static readonly DependencyProperty SearchInProperty =
			DependencyProperty.Register("SearchIn", typeof(string), typeof(SearchControl), new PropertyMetadata(null));

		private void searchField_TextChanged(object sender, TextChangedEventArgs e)
		{
			SearchReasoltsLocations = new List<int>();
			if (!string.IsNullOrEmpty(SearchIn) && !string.IsNullOrEmpty(Text))
			{
				bool toContinue = true;
				int lastIndex = 0;

				string searchValue = Text.ToLower();
				var data = SearchIn.ToLower();

				while (toContinue)
				{
					var index = data.IndexOf(searchValue, System.StringComparison.Ordinal);

					if (index == -1)
						toContinue = false;
					else
					{
						SearchReasoltsLocations.Add(index + lastIndex);
						lastIndex += index;
						data = data.Substring(index + 1);
					}
				}
			}
		}
	}
}