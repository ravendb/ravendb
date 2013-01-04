using System.Windows;
using System.Windows.Input;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
	public class DoubleClickBehavior : Behavior<FrameworkElement>
	{
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

		protected override void OnAttached()
		{
			base.OnAttached();
			AssociatedObject.MouseLeftButtonDown += UIElement_MouseLeftButtonDown;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
            
            if (AssociatedObject == null)
            {
                return;
            }
			
            AssociatedObject.MouseLeftButtonDown -= UIElement_MouseLeftButtonDown;
		}

		private void UIElement_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
		{
			if (e.ClickCount >= 2 && Command != null && Command.CanExecute(CommandParameter))
				Command.Execute(CommandParameter);
		}
	}
}