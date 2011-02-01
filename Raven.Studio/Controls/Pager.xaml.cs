namespace Raven.Studio.Controls
{
	using System.Windows;
	using System.Windows.Controls;

	public partial class Pager : UserControl
	{
		public DependencyProperty ItemsSourceProperty = DependencyProperty.Register(
			"ItemsSource",
			typeof (DataTemplate),
			typeof (Pager),
			new PropertyMetadata(null)
			);

		public Pager()
		{
			InitializeComponent();
		}

		public DataTemplate ItemsSource
		{
			get { return GetValue(ItemsSourceProperty) as DataTemplate; }
			set { SetValue(ItemsSourceProperty, value); }
		}
	}
}