namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Messages;
    using Plugin;

    public class DatabaseViewModel : Conductor<IRavenScreen>.Collection.OneActive
    {
        public DatabaseViewModel(string databaseName)
        {
            this.DisplayName = databaseName;
            this.ActivateItem(new MenuScreenViewModel());
            CompositionInitializer.SatisfyImports(this);
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

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