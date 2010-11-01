namespace Raven.Admin.ViewModels
{
    using System.ComponentModel.Composition;
    using Interfaces;

    [Export(typeof(INavigationBar))]
    public class NavigationBarViewModel : INavigationBar
    {

    }
}
