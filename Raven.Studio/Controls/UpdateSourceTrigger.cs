using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Controls
{

	public class UpdateSourceTrigger
	{
		#region TextChangeUpdateSourceTrigger

		/// <summary>
		/// TextChangeUpdateSourceTrigger Attached Dependency Property
		/// </summary>
		public static readonly DependencyProperty TextChangeUpdateSourceTriggerProperty =
			DependencyProperty.RegisterAttached("TextChangeUpdateSourceTrigger", typeof(bool), typeof(UpdateSourceTrigger),
				new PropertyMetadata((bool)false,
					new PropertyChangedCallback(OnTextChangeUpdateSourceTriggerChanged)));

		/// <summary>
		/// Gets the TextChangeUpdateSourceTrigger property. This dependency property 
		/// indicates ....
		/// </summary>
		public static bool GetTextChangeUpdateSourceTrigger(DependencyObject d)
		{
			return (bool)d.GetValue(TextChangeUpdateSourceTriggerProperty);
		}

		/// <summary>
		/// Sets the TextChangeUpdateSourceTrigger property. This dependency property 
		/// indicates ....
		/// </summary>
		public static void SetTextChangeUpdateSourceTrigger(DependencyObject d, bool value)
		{
			d.SetValue(TextChangeUpdateSourceTriggerProperty, value);
		}

		/// <summary>
		/// Handles changes to the TextChangeUpdateSourceTrigger property.
		/// </summary>
		private static void OnTextChangeUpdateSourceTriggerChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			TextBox textBox = d as TextBox;
			if (textBox == null)
				return;

			bool newTextChangeUpdateSourceTrigger = (bool)d.GetValue(TextChangeUpdateSourceTriggerProperty);

			if (newTextChangeUpdateSourceTrigger)
				textBox.TextChanged += OnTextChanged;
			else
				textBox.TextChanged -= OnTextChanged;
		}

		static void OnTextChanged(object sender, TextChangedEventArgs e)
		{
			TextBox textBox = sender as TextBox;
			var binding = textBox.GetBindingExpression(TextBox.TextProperty);
			binding.UpdateSource();
		}
		#endregion
	}
}