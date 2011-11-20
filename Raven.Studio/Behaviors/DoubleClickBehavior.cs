using System;
using System.Windows;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Threading;

namespace Raven.Studio.Behaviors
{
	public class DoubleClickBehavior : Behavior<UIElement>
	{
		private const int dblclickDelay = 200;
		private DispatcherTimer timer;

		public ICommand Command
		{
			get { return (ICommand)GetValue(CommandProperty); }
			set { SetValue(CommandProperty, value); }
		}

		public object CommandParameter
		{
			get { return (object)GetValue(CommandParameterProperty); }
			set { SetValue(CommandParameterProperty, value); }
		}


		public static readonly DependencyProperty CommandProperty = DependencyProperty.Register
			("Command", typeof(ICommand), typeof(DoubleClickBehavior), null);

		public static readonly DependencyProperty CommandParameterProperty = DependencyProperty.Register
			("CommandParameter", typeof(object), typeof(DoubleClickBehavior), null);

		public DoubleClickBehavior()
		{
			timer = new DispatcherTimer();
			timer.Interval = new TimeSpan(0, 0, 0, 0, dblclickDelay);
			timer.Tick += (sender, e) => timer.Stop();
		}

		protected override void OnAttached()
		{
			base.OnAttached();
			AssociatedObject.MouseLeftButtonDown += UIElement_MouseLeftButtonDown;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
			AssociatedObject.MouseLeftButtonDown -= UIElement_MouseLeftButtonDown;
		}

		private void UIElement_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
		{
			if (!timer.IsEnabled)
			{
				timer.Start();
				return;
			}

			timer.Stop();
			if (Command != null && Command.CanExecute(CommandParameter))
				Command.Execute(CommandParameter);
		}
	}
}