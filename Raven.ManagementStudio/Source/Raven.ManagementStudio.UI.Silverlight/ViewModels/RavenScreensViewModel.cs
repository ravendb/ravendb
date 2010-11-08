using System.Collections.ObjectModel;

namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Interfaces;

    [Export(typeof(RavenScreensViewModel))]
    public class RavenScreensViewModel : Conductor<IRavenScreen>.Collection.OneActive
    {
        public RavenScreensViewModel()
        {
            ActivateItem(new MenuScreenViewModel()
                             {
                                 DisplayName = "Menu"
                             });
        }

    }
}