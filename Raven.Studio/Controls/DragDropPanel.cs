namespace Raven.Studio.Controls
{
	using System;
	using System.Windows;
	using System.Windows.Controls;
	using System.Windows.Input;

	public class DragDropPanel : Canvas
	{
		Point beginPoint;
		Point currentPoint;
		bool dragOn;

		public DragDropPanel()
		{
			MouseLeftButtonDown += DragDropPanelMouseLeftButtonDown;
			MouseLeftButtonUp += DragDropPanelMouseLeftButtonUp;
			MouseMove += DragDropPanelMouseMove;
		}

		void DragDropPanelMouseMove(object sender, MouseEventArgs e)
		{
			if (dragOn)
			{
				currentPoint = e.GetPosition(null);
				double x0 = Convert.ToDouble(GetValue(LeftProperty));
				double y0 = Convert.ToDouble(GetValue(TopProperty));
				SetValue(LeftProperty, x0 + currentPoint.X - beginPoint.X);
				SetValue(TopProperty, y0 + currentPoint.Y - beginPoint.Y);
				beginPoint = currentPoint;
			}
		}

		void DragDropPanelMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
		{
			if (dragOn)
			{
				Opacity *= 2;
				ReleaseMouseCapture();
				dragOn = false;
			}
		}

		void DragDropPanelMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
		{
			var c = sender as FrameworkElement;
			dragOn = true;
			beginPoint = e.GetPosition(null);
			if (c != null)
			{
				c.Opacity *= 0.5;
				c.CaptureMouse();
			}
		}
	}
}