namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Plugin;

    public class MenuScreenViewModel : Screen, IRavenScreen
    {
        private IEnumerable<IPlugin> plugins;

        public MenuScreenViewModel()
        {
            this.DisplayName = "Menu";
            CompositionInitializer.SatisfyImports(this);
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        [ImportMany(AllowRecomposition = true)]
        public IEnumerable<IPlugin> Plugins
        {
            get
            {
                return this.plugins;
            } 

            set
            {
                this.plugins = value; 
                NotifyOfPropertyChange(() => this.Plugins);
            }
        }

        public void ChangeView(object view)
        {
        }
    }
}