using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace Raven.Studio.Behaviors
{
	public static class KeyBoard
	{
		public static bool IsCtrlHold { get; set; }

		public static void Register(FrameworkElement element)
		{
			element.KeyDown += OnElementOnKeyDown;
			element.KeyUp += OnElementOnKeyUp;
			element.Unloaded += Unregister;

			var childWindow = element as ChildWindow;
			if (childWindow != null)
			{
				childWindow.Closed += Unregister;
			}
		}

		private static void Unregister(object sender, EventArgs routedEventArgs)
		{
			var element = (FrameworkElement) sender;
			element.KeyDown -= OnElementOnKeyDown;
			element.KeyUp -= OnElementOnKeyUp;
			element.Unloaded -= Unregister;

			var childWindow = element as ChildWindow;
			if (childWindow != null)
			{
				childWindow.Closed -= Unregister;
			}
		}

		private static void OnElementOnKeyDown(object sender, KeyEventArgs args)
		{
			if (args.Key == Key.Ctrl)
				IsCtrlHold = true;
		}

		private static void OnElementOnKeyUp(object sender, KeyEventArgs args)
		{
			if (args.Key == Key.Ctrl)
				IsCtrlHold = false;
		}
	}
}