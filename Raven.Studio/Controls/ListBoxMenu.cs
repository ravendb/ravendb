using System.Windows.Controls;
using System.Windows.Interactivity;
using Raven.Studio.Behaviors;

namespace Raven.Studio.Controls
{
	public class ListBoxMenu : ListBox
	{
		public ListBoxMenu()
		{
			Interaction.GetBehaviors(this).Add(new SelectItemOnRightClick());	
		}
	}
}