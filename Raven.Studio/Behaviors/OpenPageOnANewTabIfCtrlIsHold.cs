using System;
using System.Windows.Browser;
using System.Windows.Controls;
using System.Windows.Interactivity;
using System.Windows.Navigation;

namespace Raven.Studio.Behaviors
{
	public class OpenPageOnANewTabIfCtrlIsHold : Behavior<Frame>
	{
		protected override void OnAttached()
		{
			base.OnAttached();
			AssociatedObject.Navigating += AssociatedObjectOnNavigating;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
			AssociatedObject.Navigating -= AssociatedObjectOnNavigating;
		}

		private void AssociatedObjectOnNavigating(object sender, NavigatingCancelEventArgs args)
		{
			var url = args.Uri.OriginalString;
			if (string.IsNullOrEmpty(url) || url.StartsWith("http://"))
				return;

			if (KeyboardBehavior.IsCtrlHold == false)
				return;

			var hostUrl = HtmlPage.Document.DocumentUri.OriginalString;
			var fregmentIndex = hostUrl.IndexOf('#');
			string host = fregmentIndex != -1 ? hostUrl.Substring(0, fregmentIndex) : hostUrl;

			HtmlPage.Window.Navigate(new Uri(host + "#" + url, UriKind.Absolute), "_blank");
			args.Cancel = true;
		}
	}
}