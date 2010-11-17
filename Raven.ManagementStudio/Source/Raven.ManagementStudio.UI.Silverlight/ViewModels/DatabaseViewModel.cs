namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System;
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Messages;
    using Models;
    using Plugin;
    using Screens;
    using System.ComponentModel.Composition.Hosting;

    public class DatabaseViewModel : Conductor<IRavenScreen>.Collection.OneActive, IHandle<ReplaceActiveScreen>
    {
        public DatabaseViewModel(IDatabase database)
        {
            this.Database = database;
            this.DisplayName = this.Database.Name;
            this.ActivateItem(new MenuScreenViewModel(this.Database));
            CompositionInitializer.SatisfyImports(this);
            this.EventAggregator.Subscribe(this);
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public IDatabase Database { get; set; }

        public override sealed void ActivateItem(IRavenScreen item)
        {
            if (this.EventAggregator != null)
            {
                this.EventAggregator.Publish(new ChangeActiveScreen(item));
            }

            base.ActivateItem(item);
        }

        public void Handle(ReplaceActiveScreen message)
        {
            if (message.NewScreen.ParentRavenScreen == this.ActiveItem)
            {
                var index = this.Items.IndexOf(this.ActiveItem);
                this.CloseItem(this.ActiveItem);
                this.Items.Insert(index, message.NewScreen);
                this.ActiveItem = message.NewScreen;
            }
        }
    }
}