namespace Raven.Studio.Behaviors
{
	using System.Windows;
	using System.Windows.Controls;
	using System.Windows.Interactivity;
	using System.Windows.Media;

	public class Watermark : Behavior<TextBox>
	{
		bool _hasWatermark;

		Brush _textBoxForeground;

		public string Text { get; set; }

		public Brush Foreground { get; set; }

		protected override void OnAttached()
		{
			_textBoxForeground = AssociatedObject.Foreground;

			base.OnAttached();
			if (Text != null)
			{
				SetWatermarkText();
			}

			AssociatedObject.GotFocus += GotFocus;
			AssociatedObject.LostFocus += LostFocus;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
			AssociatedObject.GotFocus -= GotFocus;
			AssociatedObject.LostFocus -= LostFocus;
		}

		void LostFocus(object sender, RoutedEventArgs e)
		{
			if (AssociatedObject.Text.Length == 0)
			{
				if (Text != null)
				{
					SetWatermarkText();
				}
			}
		}

		void GotFocus(object sender, RoutedEventArgs e)
		{
			if (_hasWatermark)
			{
				RemoveWatermarkText();
			}
		}

		void RemoveWatermarkText()
		{
			AssociatedObject.Foreground = _textBoxForeground;
			AssociatedObject.Text = string.Empty;
			_hasWatermark = false;
		}

		void SetWatermarkText()
		{
			AssociatedObject.Foreground = Foreground;
			AssociatedObject.Text = Text;
			_hasWatermark = true;
		}
	}
}