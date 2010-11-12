namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Messages;
    using Models;
    using Plugin;

    public class DatabaseViewModel : Conductor<IRavenScreen>.Collection.OneActive
    {
        public DatabaseViewModel(Database database)
        {
            this.Database = database;
            this.DisplayName = this.Database.Name;
            this.ActivateItem(new MenuScreenViewModel(this.Database));
            CompositionInitializer.SatisfyImports(this);
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public Database Database { get; set; }

        public override sealed void ActivateItem(IRavenScreen item)
        {
            if (this.EventAggregator != null)
            {
                this.EventAggregator.Publish(new ActiveScreenChanged(item));
            }

            base.ActivateItem(item);
        }
    }
}