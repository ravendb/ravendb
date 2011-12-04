using System;
using System.ComponentModel;
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

		protected override void OnAttached()
		{
			AssociatedObject.SizeChanged += AssociatedObjectOnSizeChanged;
			AssociatedObject.Loaded += AssociatedObjectOnLoaded;
			DocumentsModel.DocumentSize.PropertyChanged += DocumentSizeOnPropertyChanged;
		}

		protected override void OnDetaching()
		{
			AssociatedObject.SizeChanged -= AssociatedObjectOnSizeChanged;
			AssociatedObject.Loaded -= AssociatedObjectOnLoaded;
			DocumentsModel.DocumentSize.PropertyChanged -= DocumentSizeOnPropertyChanged;

		}

		private void AssociatedObjectOnSizeChanged(object sender, SizeChangedEventArgs sizeChangedEventArgs)
		{
			CalculatePageSize();
		}

		private void AssociatedObjectOnLoaded(object sender, RoutedEventArgs routedEventArgs)
		{
			CalculatePageSize();
		}

		private void DocumentSizeOnPropertyChanged(object sender, PropertyChangedEventArgs propertyChangedEventArgs)
		{
			CalculatePageSize();
		}

		private void CalculatePageSize()
		{
			if (Model == null)
				return;

			var row = AssociatedObject.ActualWidth/DocumentsModel.DocumentSize.Width;
			var column = AssociatedObject.ActualHeight/DocumentsModel.DocumentSize.Height;
			Model.Pager.PageSize = (int) (row*column);
		}
	}
}