using System.Windows;
using System.Windows.Input;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
	public class KeyboardBehavior : Behavior<UIElement>
	{
		protected override void OnAttached()
		{
			base.OnAttached();
			AssociatedObject.KeyDown +=AssociatedObjectOnKeyDown;
			AssociatedObject.KeyUp +=AssociatedObjectOnKeyUp;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
			AssociatedObject.KeyDown -= AssociatedObjectOnKeyDown;
			AssociatedObject.KeyUp -= AssociatedObjectOnKeyUp;
		}

		private void AssociatedObjectOnKeyDown(object sender, KeyEventArgs args)
		{
			if (args.Key == Key.Ctrl)
				IsCtrlHold = true;
		}

		private void AssociatedObjectOnKeyUp(object sender, KeyEventArgs args)
		{
			if (args.Key == Key.Ctrl)
				IsCtrlHold = false;
		}

		public static bool IsCtrlHold { get; set; }
	}
}