using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Behaviors
{
	public class RaiseCanExecuteOnSelectionChanged : StudioBehavior<Selector>
	{
		public ButtonBase CommandButton
		{
			get { return (ButtonBase)GetValue(CommandButtonProperty); }
			set { SetValue(CommandButtonProperty, value); }
		}

		public static readonly DependencyProperty CommandButtonProperty =
			DependencyProperty.Register("CommandButton", typeof(ButtonBase), typeof(RaiseCanExecuteOnSelectionChanged), null);
		
		private void OnSelectionChanged(object sender, SelectionChangedEventArgs selectionChangedEventArgs)
		{
			if (CommandButton == null)
				return;
			var command = CommandButton.Command as Command;
			if (command == null)
				return;
			command.RaiseCanExecuteChanged();
		}

		protected override void OnAttached()
		{
			base.OnAttached();
			AssociatedObject.SelectionChanged += OnSelectionChanged;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
			AssociatedObject.SelectionChanged -= OnSelectionChanged;
		}
	}
}