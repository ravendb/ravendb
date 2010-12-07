using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Caliburn.Micro;
using Raven.ManagementStudio.Plugin;

namespace Raven.ManagementStudio.UI.Silverlight.ViewModels.Screens
{
    public class MenuScreenViewModel : Screen, IRavenScreen
    {
        private IEnumerable<IPlugin> _plugins;
        private IEnumerable<IPlugin> _documentPlugins;
        private IEnumerable<IPlugin> _collectionPlugins;
        private IEnumerable<IPlugin> _indexPlugins;
        private IEnumerable<IPlugin> _statisticPlugins;
        private IEnumerable<IPlugin> _administrationPlugins;
        private IEnumerable<IPlugin> _otherPlugins;
        private bool _isBusy;

        public MenuScreenViewModel(IDatabase database)
        {
            Database = database;
            DisplayName = "Home";
            CompositionInitializer.SatisfyImports(this);
        }

        public bool IsBusy
        {
            get { return _isBusy; }
            set
            {
                _isBusy = value;
                NotifyOfPropertyChange(() => IsBusy);
            }
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        [ImportMany(AllowRecomposition = true)]
        public IEnumerable<IPlugin> Plugins
        {
            get { return _plugins; }
            set
            {
                _plugins = value;

                foreach (var plugin in _plugins)
                {
                    plugin.Database = Database;
                }

                NotifyOfPropertyChange(() => Plugins);
                NotifyOfPropertyChange(() => DocumentPlugins);
                NotifyOfPropertyChange(() => CollectionPlugins);
                NotifyOfPropertyChange(() => IndexPlugins);
                NotifyOfPropertyChange(() => StatisticPlugins);
                NotifyOfPropertyChange(() => AdministrationPlugins);
                NotifyOfPropertyChange(() => OtherPlugins);
                SetCurrentPlugins();
            }
        }

        private IEnumerable<IPlugin> _currentPlugins;
        public IEnumerable<IPlugin> CurrentPlugins
        {
            get { return _currentPlugins; }
            private set
            {
                _currentPlugins = value;
                NotifyOfPropertyChange(() => CurrentPlugins);                
            }
        }

        private void SetCurrentPlugins()
        {
            CurrentPlugins = Plugins
                .Where(p => p.Section == CurrentSectionType)
                .OrderBy(p => p.Ordinal)
                .ToArray();
        }

        public void Activate(IRavenScreen screen)
        {
            if (_currentPlugins != null)
            {
                foreach (var plugin in _currentPlugins)
                {
                    plugin.IsActive = plugin.RelatedScreen.GetType() == screen.GetType();
                }
            }
        }

        public IEnumerable<IPlugin> DocumentPlugins
        {
            get { return _documentPlugins ?? (_documentPlugins = Plugins.Where(x => x.Section == SectionType.Documents)); }
        }

        public IEnumerable<IPlugin> CollectionPlugins
        {
            get { return _collectionPlugins ?? (_collectionPlugins = Plugins.Where(x => x.Section == SectionType.Collections)); }
        }

        public IEnumerable<IPlugin> IndexPlugins
        {
            get { return _indexPlugins ?? (_indexPlugins = Plugins.Where(x => x.Section == SectionType.Indexes)); }
        }

        public IEnumerable<IPlugin> StatisticPlugins
        {
            get { return _statisticPlugins ?? (_statisticPlugins = Plugins.Where(x => x.Section == SectionType.Statistics)); }
        }

        public IEnumerable<IPlugin> AdministrationPlugins
        {
            get { return _administrationPlugins ?? (_administrationPlugins = Plugins.Where(x => x.Section == SectionType.Administration)); }
        }

        public IEnumerable<IPlugin> OtherPlugins
        {
            get { return _otherPlugins ?? (_otherPlugins = Plugins.Where(x => x.Section == SectionType.Other)); }
        }

        public IDatabase Database { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public SectionType Section { get { return SectionType.None; } }

        private SectionType _currentSectionType;
        public SectionType CurrentSectionType
        {
            get { return _currentSectionType; }
            set
            {
                _currentSectionType = value;
                SetCurrentPlugins();
            }
        }
    }
}
