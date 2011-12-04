using System;
using System.Reactive.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Interactivity;
using Raven.Studio.Models;

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
			var events = Observable.FromEventPattern<EventArgs>(AssociatedObject, "SizeChanged")
				.Concat(Observable.FromEventPattern<EventArgs>(DocumentsModel.DocumentSize, "PropertyChanged"))
				.Throttle(TimeSpan.FromSeconds(0.5))
				.Concat(Observable.FromEventPattern<EventArgs>(AssociatedObject, "Loaded")) // Loaded should execute immediately.
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

			var row = AssociatedObject.ActualWidth / DocumentsModel.DocumentSize.Width;
			var column = AssociatedObject.ActualHeight / DocumentsModel.DocumentSize.Height;
			Model.Pager.PageSize = (int)(row * column);
		}
	}
}