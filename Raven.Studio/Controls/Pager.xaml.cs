namespace Raven.Studio.Controls
{
	using System;
	using System.Windows;
	using System.Windows.Controls;
	using Framework;
	using Action = Caliburn.Micro.Action;

	public partial class Pager : UserControl
	{
		public static readonly DependencyProperty ItemSizeProperty = DependencyProperty.Register(
			"ItemSize",
			typeof (double),
			typeof (Pager),
			new PropertyMetadata(0.0));

		public static readonly DependencyProperty ItemsSourceProperty = DependencyProperty.Register(
			"ItemsSource",
			typeof(IBindablePagedQuery),
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
		/// The size of the elements that will be contained in page. Specifically, the element's height.
		/// </summary>
		/// <remarks>
		/// If the size is unknown, we can't dynamically determine how big to make the request
		/// </remarks>
		public double ItemSize
		{
			get { return (double) GetValue(ItemSizeProperty); }
			set { SetValue(ItemSizeProperty, value); }
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
			get { return (IBindablePagedQuery)GetValue(ItemsSourceProperty); }
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

			source.ItemSize = Convert.ToDouble(d.GetValue(ItemSizeProperty));
			source.HeightOfPage = container.ActualHeight;
		}

		static void PageContainerChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			if (e.NewValue == null) return;

			var el = (FrameworkElement) e.NewValue;
			el.SizeChanged += delegate { AttemptToCalculatePageSize(d); };
		}
	}
}