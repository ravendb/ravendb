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
			get { return (DocumentsModel)GetValue(ModelProperty); }
			set { SetValue(ModelProperty, value); }
		}

		public static readonly DependencyProperty ModelProperty =
			DependencyProperty.Register("Model", typeof(DocumentsModel), typeof(ShowItemsBasedOnControlDimensions), null);

		protected override void OnAttached()
		{
			AssociatedObject.SizeChanged += AssociatedObjectOnSizeChanged;
		}

		protected override void OnDetaching()
		{
			AssociatedObject.SizeChanged -= AssociatedObjectOnSizeChanged;
		}

		private void AssociatedObjectOnSizeChanged(object sender, SizeChangedEventArgs sizeChangedEventArgs)
		{
			var row = AssociatedObject.ActualWidth/DocumentsModel.DocumentSize.Width;
			var column = AssociatedObject.ActualHeight/DocumentsModel.DocumentSize.Height;
			Model.Pager.PageSize = (int) (row*column);
		}
	}
}