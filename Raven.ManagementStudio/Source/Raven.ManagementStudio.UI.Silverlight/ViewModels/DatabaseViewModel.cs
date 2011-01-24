using System;
using System.Linq;
using System.ComponentModel.Composition;
using Caliburn.Micro;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Messages;
using Raven.ManagementStudio.UI.Silverlight.ViewModels.Screens;

namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    public class DatabaseViewModel : Conductor<IRavenScreen>.Collection.OneActive, IHandle<ReplaceActiveScreen>
    {
        public DatabaseViewModel(IDatabase database)
        {
            Database = database;
            DisplayName = Database.Name;
            Menu = new MenuScreenViewModel(Database);
            Home = new HomeScreenViewModel(Database);
            GoHome();
            CompositionInitializer.SatisfyImports(this);
            EventAggregator.Subscribe(this);
        }

        public MenuScreenViewModel Menu { get; private set; }

        public HomeScreenViewModel Home { get; set; }

        public void GoHome()
        {
            ViewPlugins(SectionType.None);
            ActivateItem(Home);
        }

        public void GoBack()
        {
            if (ActiveItem != null && Items.Count > 1)
            {
                Items.RemoveAt(Items.Count - 1);
                ActivateItem(Items.Last());
            }
        }

        public void ViewPlugins(string sectionTypeString)
        {
            SectionType sectionType;
            Enum.TryParse(sectionTypeString, out sectionType);
            ViewPlugins(sectionType);
        }

        private void ViewPlugins(SectionType sectionType)
        {
            Menu.CurrentSectionType = sectionType;
            var first = Menu.CurrentPlugins.FirstOrDefault();
            if (first != null)
            {
                first.GoToScreen();
            }
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public IDatabase Database { get; set; }

        public override sealed void ActivateItem(IRavenScreen item)
        {
            base.ActivateItem(item);

            Menu.Activate(item);
        }

        public void Handle(ReplaceActiveScreen message)
        {
            //if (message.NewScreen.ParentRavenScreen == ActiveItem)
            //{
            //    var index = Items.IndexOf(ActiveItem);
            //    CloseItem(ActiveItem);
            //    Ittems.Insert(index, message.NewScreen);
            //    ActiveItem = message.NewScreen;
            //}
            ActivateItem(message.NewScreen);
        }
    }
}