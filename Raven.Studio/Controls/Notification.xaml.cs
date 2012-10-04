using System;
using System.Windows.Controls;
using System.Windows.Media.Animation;

namespace Raven.Studio
{
	public partial class NotificationView : UserControl
	{
        public NotificationView()
		{
			// Required to initialize variables
			InitializeComponent();
		}

        public void Display(bool replaceExisting)
        {
            var storyboardName = replaceExisting ? "FadeIn" : "ScaleIn";
            var storyboard = Resources[storyboardName] as Storyboard;
            storyboard.Begin();
        }

        public void Hide(Action onHidden)
        {
            var storyboard = Resources["FadeOut"] as Storyboard;
            storyboard.Begin();

            EventHandler handler = null;
            handler = delegate
                          {
                              onHidden();
                              storyboard.Completed -= handler;
                          };

            storyboard.Completed += handler;
        }
	}
}