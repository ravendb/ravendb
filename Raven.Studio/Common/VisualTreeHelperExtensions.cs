using System.Windows;
using System.Windows.Media;

namespace Raven.Studio.Common
{
	public static class VisualTreeHelperExtensions
	{
		 public static T GetParentOfType<T>(DependencyObject element) where T : DependencyObject
		 {
		 	var parent = VisualTreeHelper.GetParent(element);
		 	while (parent != null)
		 	{
		 		var found = parent as T;
		 		if (found != null)
		 			return found;
		 		
				parent = VisualTreeHelper.GetParent(parent);
		 	}
		 	return null;
		 }
	}
}