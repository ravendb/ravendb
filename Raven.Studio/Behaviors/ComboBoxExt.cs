using System.Windows;

namespace Raven.Studio.Behaviors
{
	public class ComboBoxExt
	{
		public static DataTemplate GetSelectionBoxItemTemplate(DependencyObject obj)
		{
			return (DataTemplate)obj.GetValue(SelectionBoxItemTemplateProperty);
		}

		public static void SetSelectionBoxItemTemplate(DependencyObject obj, DataTemplate value)
		{
			obj.SetValue(SelectionBoxItemTemplateProperty, value);
		}

		public static readonly DependencyProperty SelectionBoxItemTemplateProperty =
			DependencyProperty.RegisterAttached("SelectionBoxItemTemplate", typeof(DataTemplate), typeof(ComboBoxExt),
												new PropertyMetadata(null));
	}
}