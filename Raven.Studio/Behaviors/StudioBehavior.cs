using System.Windows;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
	public class StudioBehavior<T> : Behavior<T> where T : FrameworkElement
	{
		protected override void OnAttached()
		{
			AssociatedObject.Unloaded += (sender, args) => Detach();
		}
	}
}