using System.Linq;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using SL4PopupMenu;

namespace Raven.Studio.Behaviors
{
	public class AttachMenu : Behavior<ListBox>
	{
		public PopupMenu TargetMenu { get; set; }

		protected override void OnAttached()
		{
			//TargetMenu.OpenNextTo(MenuOrientationTypes.MouseBottomRight, null, true, true);

			base.OnAttached();
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
		}
	}
}