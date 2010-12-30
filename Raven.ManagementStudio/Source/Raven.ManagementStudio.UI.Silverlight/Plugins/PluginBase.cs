using System.ComponentModel.Composition;
using Caliburn.Micro;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Messages;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins
{
    public abstract class PluginBase : PropertyChangedBase, IPlugin
    {
        public abstract SectionType Section { get; }

        public abstract IRavenScreen RelatedScreen { get; }

        public abstract string Name { get; }

        public virtual object MenuView
        {
            get { return null; }
        }

        public virtual int Ordinal
        {
            get { return 0; }
        }

        public IDatabase Database { get; set; }

        public void GoToScreen()
        {
            EventAggregator.Publish(new OpenNewScreen(RelatedScreen));
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        private bool _isActive;
        public bool IsActive
        {
            get { return _isActive; }
            set
            {
                _isActive = value;
                NotifyOfPropertyChange(() => IsActive);
            }
        }
    }
}
