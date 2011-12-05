using System;
using System.Reactive.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Interactivity;
using Raven.Studio.Models;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Behaviors
{
	public class ShowItemsBasedOnControlDimensions : Behavior<ListBox>
	{
		public DocumentsModel Model
		{
			get { return (DocumentsModel) GetValue(ModelProperty); }
			set { SetValue(ModelProperty, value); }
		}

		public static readonly DependencyProperty ModelProperty =
			DependencyProperty.Register("Model", typeof (DocumentsModel), typeof (ShowItemsBasedOnControlDimensions), null);

		private IDisposable disposable;

		protected override void OnAttached()
		{
			var events = Observable.FromEventPattern<SizeChangedEventHandler, SizeChangedEventArgs>(e => AssociatedObject.SizeChanged += e, e => AssociatedObject.SizeChanged -= e).NoSignature()
				.Merge(Observable.FromEventPattern<EventHandler, EventArgs>(e => DocumentsModel.DocumentSize.SizeChanged += e, e => DocumentsModel.DocumentSize.SizeChanged -= e))
				.Throttle(TimeSpan.FromSeconds(0.5))
				.Merge(Observable.FromEventPattern<RoutedEventHandler, EventArgs>(e => AssociatedObject.Loaded += e, e => AssociatedObject.Loaded -= e))  // Loaded should execute immediately.
				.ObserveOnDispatcher();

			disposable = events.Subscribe(_ => CalculatePageSize());
		}

		protected override void OnDetaching()
		{
			if (disposable != null)
				disposable.Dispose();
		}

		private void CalculatePageSize()
		{
			if (Model == null)
				return;

			// ReSharper disable CompareOfFloatsByEqualityOperator
			if (AssociatedObject.ActualWidth == 0)
				return;
			// ReSharper restore CompareOfFloatsByEqualityOperator

			int row = (int) (AssociatedObject.ActualWidth / (DocumentsModel.DocumentSize.Width + 28));
			int column = (int) (AssociatedObject.ActualHeight / (DocumentsModel.DocumentSize.Height + 24));
			Model.Pager.PageSize = row * column;
			var a = row;
		}
	}
}