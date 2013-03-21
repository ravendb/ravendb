using System.Windows;

namespace RavenFS.Studio.Behaviors
{
	/// <summary>
	/// Extend Microsoft's DataTrigger by evaluating the condition when the element loads
	/// </summary>
	public class StudioDataTrigger : Microsoft.Expression.Interactivity.Core.DataTrigger
	{
		protected override void OnAttached()
		{
			base.OnAttached();
			var element = AssociatedObject as FrameworkElement;
			if (element != null)
			{
				element.Loaded += OnElementLoaded;
			}
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
			var element = AssociatedObject as FrameworkElement;
			if (element != null)
			{
				element.Loaded -= OnElementLoaded;
			}
		}

		private void OnElementLoaded(object sender, RoutedEventArgs e)
		{
			EvaluateBindingChange(null);
		}
	}
}
