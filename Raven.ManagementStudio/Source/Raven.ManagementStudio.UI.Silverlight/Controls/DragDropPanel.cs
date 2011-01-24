namespace Raven.ManagementStudio.UI.Silverlight.Controls
{
    using System.Windows;
    using System.Windows.Controls;
    using System.Windows.Input;

    public class DragDropPanel : Canvas
    {
        private Point beginPoint;
        private Point currentPoint;
        private bool dragOn;

        public DragDropPanel()
        {
            this.MouseLeftButtonDown += this.DragDropPanelMouseLeftButtonDown;
            this.MouseLeftButtonUp += this.DragDropPanelMouseLeftButtonUp;
            this.MouseMove += this.DragDropPanelMouseMove;  
        }

        private void DragDropPanelMouseMove(object sender, MouseEventArgs e)
        {
            if (this.dragOn)
            {
                this.currentPoint = e.GetPosition(null);
                var x0 = System.Convert.ToDouble(this.GetValue(LeftProperty));
                var y0 = System.Convert.ToDouble(this.GetValue(TopProperty));
                this.SetValue(LeftProperty, x0 + this.currentPoint.X - this.beginPoint.X);
                this.SetValue(TopProperty, y0 + this.currentPoint.Y - this.beginPoint.Y);
                this.beginPoint = this.currentPoint;
            }
        }

        private void DragDropPanelMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
            if (this.dragOn)
            {
                this.Opacity *= 2;
                this.ReleaseMouseCapture();
                this.dragOn = false;
            }
        }

        private void DragDropPanelMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            var c = sender as FrameworkElement;
            this.dragOn = true;
            this.beginPoint = e.GetPosition(null);
            if (c != null)
            {
                c.Opacity *= 0.5;
                c.CaptureMouse();
            }
        }
    }
}