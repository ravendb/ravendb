namespace Raven.Studio.Controls
{
	using System;
	using System.Windows;
	using System.Windows.Controls;
	using Framework;
	using Action = Caliburn.Micro.Action;

	public partial class Pager : UserControl
	{
		public static readonly DependencyProperty ItemHeightProperty = DependencyProperty.Register(
			"ItemHeight",
			typeof (double),
			typeof (Pager),
			new PropertyMetadata(66.0));

		public static readonly DependencyProperty ItemWidthProperty = DependencyProperty.Register(
			"ItemWidth",
			typeof (double),
			typeof (Pager),
			new PropertyMetadata(126.0));

		public static readonly DependencyProperty ItemsSourceProperty = DependencyProperty.Register(
			"ItemsSource",
			typeof (IBindablePagedQuery),
			typeof (Pager),
			new PropertyMetadata(null, ItemsSourceChanged));

		public static readonly DependencyProperty PageContainerProperty = DependencyProperty.Register(
			"PageContainer",
			typeof (FrameworkElement),
			typeof (Pager),
			new PropertyMetadata(null, PageContainerChanged));

		public Pager()
		{
			InitializeComponent();
		}

		/// <summary>
		/// The height of the elements that will be contained in page
		/// </summary>
		/// <remarks>
		/// If the size is unknown, we can't dynamically determine how big to make the request
		/// </remarks>
		public double ItemHeight
		{
			get { return (double)GetValue(ItemHeightProperty); }
			set { SetValue(ItemHeightProperty, value); }
		}

		/// <summary>
		/// The width of the elements that will be contained in page
		/// </summary>
		/// <remarks>
		/// If the size is unknown, we can't dynamically determine how big to make the request
		/// </remarks>
		public double ItemWidth
		{
			get { return (double)GetValue(ItemWidthProperty); }
			set { SetValue(ItemWidthProperty, value); }
		}

		/// <summary>
		/// The element that contains the entire page.
		/// </summary>
		public FrameworkElement PageContainer
		{
			get { return (FrameworkElement) GetValue(PageContainerProperty); }
			set { SetValue(PageContainerProperty, value); }
		}

		/// <summary>
		/// The bindable paged query that underlies the pager
		/// </summary>
		public IBindablePagedQuery ItemsSource
		{
			get { return (IBindablePagedQuery) GetValue(ItemsSourceProperty); }
			set { SetValue(ItemsSourceProperty, value); }
		}

		static void ItemsSourceChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			var pager = (Pager) d;
			Action.SetTarget(pager.LayoutRoot, e.NewValue);
		}

		static void AttemptToCalculatePageSize(DependencyObject d)
		{
			var container = d.GetValue(PageContainerProperty) as FrameworkElement;
			var source = d.GetValue(ItemsSourceProperty) as IBindablePagedQuery;

			if (source == null || container == null) return;

			var itemH = Convert.ToDouble(d.GetValue(ItemHeightProperty));
			var itemW = Convert.ToDouble(d.GetValue(ItemWidthProperty));
			source.ItemElementSize = new Size(itemW,itemH);

			source.PageElementSize = new Size(container.ActualWidth, container.ActualHeight);

			source.AdjustResultsForPageSize();
		}

		static void PageContainerChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			if (e.NewValue == null) return;

			var el = (FrameworkElement) e.NewValue;
			el.SizeChanged += delegate { AttemptToCalculatePageSize(d); };
		}
	}
}