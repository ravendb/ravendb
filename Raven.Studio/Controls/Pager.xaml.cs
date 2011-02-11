namespace Raven.Studio.Controls
{
	using System;
	using System.ComponentModel;
	using System.Windows;
	using System.Windows.Controls;
	using Framework;

	//TODO: this needs some serious work. The page is to analyze the size of the page's container
	// and if the size of the individual elements can be predicated to dynamically adjust the request page size.
	// tabling the work until more important features are complete
	public partial class Pager : UserControl
	{
		//public static readonly DependencyProperty ItemSizeProperty = DependencyProperty.Register(
		//    "ItemSize",
		//    typeof (double),
		//    typeof (Pager),
		//    new PropertyMetadata(0));

		public static readonly DependencyProperty PageContainerProperty = DependencyProperty.Register(
			"PageContainer",
			typeof(FrameworkElement),
			typeof(Pager),
			new PropertyMetadata(null, PageContainerChanged));

		public static readonly DependencyProperty WatcherProperty = DependencyProperty.Register(
			"Watcher",
			typeof(object),
			typeof(Pager),
			new PropertyMetadata(null, DataContextChangedEx));

		public object Watcher
		{
			get { return GetValue(WatcherProperty); }
			set { SetValue(WatcherProperty, value); }
		}

		static void DataContextChangedEx(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			//var change = e.NewValue as IBindablePagedQuery;
			//if (change != null)
			//{
			//    change.HeightOfPage = ((FrameworkElement)d.GetValue(PageContainerProperty)).ActualHeight;
			//    change.ItemSize = 81;
			//}
		}

		public Pager()
		{
			InitializeComponent();
		}

		//public double ItemSize
		//{
		//    get { return (double) GetValue(ItemSizeProperty); }
		//    set { SetValue(ItemSizeProperty, value); }
		//}

		public FrameworkElement PageContainer
		{
			get { return (FrameworkElement)GetValue(PageContainerProperty); }
			set { SetValue(PageContainerProperty, value); }
		}

		static void PageContainerChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			
		}

		private void TextBox_TextChanged(object sender, TextChangedEventArgs e)
		{

		}
	}
}