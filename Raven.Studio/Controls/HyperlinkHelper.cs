namespace Raven.Studio.Controls
{
	using System.Windows;
	using System.Windows.Controls;

	public class HyperlinkHelper
	{
		public static readonly DependencyProperty IsActiveProperty =
			DependencyProperty.RegisterAttached("IsActive", typeof (bool?), typeof (HyperlinkHelper),
			                                    new PropertyMetadata(null, OnIsActiveChanged));

		public static bool? GetIsActive(DependencyObject obj)
		{
			return (bool?) obj.GetValue(IsActiveProperty);
		}

		public static void SetIsActive(DependencyObject obj, bool? value)
		{
			obj.SetValue(IsActiveProperty, value);
		}

		static void OnIsActiveChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			var control = (Control) d;
			if (!SetState(control, (bool?) e.NewValue))
			{
				control.Loaded += OnControlLoaded;
			}
		}

		static void OnControlLoaded(object sender, RoutedEventArgs e)
		{
			var control = (Control) sender;
			control.Loaded -= OnControlLoaded;
			SetState(control, GetIsActive(control));
		}

		static bool SetState(Control control, bool? value)
		{
			return VisualStateManager.GoToState(control, value == true ? "ActiveLink" : "InactiveLink", true);
		}
	}
}