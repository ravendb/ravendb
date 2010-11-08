using System.ComponentModel.Composition;
using Raven.ManagementStudio.UI.Silverlight.ViewModels.Interfaces;

namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    [Export(typeof(INavigationBar))]
    public class NavigationBarViewModel : INavigationBar
    {

    }
}
