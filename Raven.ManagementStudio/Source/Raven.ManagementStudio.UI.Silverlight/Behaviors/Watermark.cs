namespace Raven.ManagementStudio.UI.Silverlight.Behaviors
{
    using System.Windows;
    using System.Windows.Controls;
    using System.Windows.Interactivity;
    using System.Windows.Media;

    public class Watermark : Behavior<TextBox>
    {
        private bool hasWatermark;

        private Brush textBoxForeground;

        public string Text { get; set; }

        public Brush Foreground { get; set; }

        protected override void OnAttached()
        {
            this.textBoxForeground = AssociatedObject.Foreground;

            base.OnAttached();
            if (this.Text != null)
            {
                this.SetWatermarkText();
            }

            AssociatedObject.GotFocus += this.GotFocus;
            AssociatedObject.LostFocus += this.LostFocus;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();
            AssociatedObject.GotFocus -= this.GotFocus;
            AssociatedObject.LostFocus -= this.LostFocus;
        }

        private void LostFocus(object sender, RoutedEventArgs e)
        {
            if (AssociatedObject.Text.Length == 0)
            {
                if (this.Text != null)
                {
                    this.SetWatermarkText();
                }
            }
        }

        private void GotFocus(object sender, RoutedEventArgs e)
        {
            if (this.hasWatermark)
            {
                this.RemoveWatermarkText();
            }
        }

        private void RemoveWatermarkText()
        {
            AssociatedObject.Foreground = this.textBoxForeground;
            AssociatedObject.Text = string.Empty;
            this.hasWatermark = false;
        }

        private void SetWatermarkText()
        {
            AssociatedObject.Foreground = this.Foreground;
            AssociatedObject.Text = this.Text;
            this.hasWatermark = true;
        }
    }
}