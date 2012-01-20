using System.Windows;
using Microsoft.Expression.Interactivity.Input;

namespace Raven.Studio.Behaviors
{
	public class StudioKeyTrigger : KeyTrigger 
	{
		protected override void OnAttached()
		{
			base.OnAttached();

			var element = AssociatedObject as FrameworkElement;
			if (element == null)
				return;

			element.Unloaded += (sender, args) => Detach();
		}
	}
}