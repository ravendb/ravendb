using System.Windows.Controls;
using Raven.Studio.Behaviors;

namespace Raven.Studio.Infrastructure
{
	public class PopupWindow : ChildWindow
	{
		public PopupWindow()
		{
			KeyBoard.IsCtrlHold = false;
		}
	}
}