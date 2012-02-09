using System;
using System.Windows;
using System.Windows.Input;

namespace Raven.Studio.Behaviors
{
	public static class KeyBoard
	{
		public static bool IsCtrlHold { get; set; }

		public static void Register(UIElement element)
		{
			element.KeyDown += (sender, args) =>
			{
				if (args.Key == Key.Ctrl)
					IsCtrlHold = true;
			};
			element.KeyUp += (sender, args) =>
			{
				if (args.Key == Key.Ctrl)
					IsCtrlHold = false;
			};
		}
	}
}