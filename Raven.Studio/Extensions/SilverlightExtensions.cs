using System.Windows;
using System.Windows.Media;

namespace Raven.Studio.Extensions
{
	public static class SilverlightExtensions
	{
		public static DependencyObject GetRoot(this DependencyObject child)
		{
			var parent = VisualTreeHelper.GetParent(child);
			if (parent != null)
				GetRoot(parent);
			return child;
		}
	}
}