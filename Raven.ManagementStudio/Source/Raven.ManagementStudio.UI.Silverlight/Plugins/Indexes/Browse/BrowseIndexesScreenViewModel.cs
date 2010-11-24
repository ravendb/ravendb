namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Linq;
    using Caliburn.Micro;
    using Raven.Database.Indexing;
    using Raven.ManagementStudio.Plugin;
    using Raven.ManagementStudio.UI.Silverlight.Dialogs;
    using Raven.ManagementStudio.UI.Silverlight.Models;

    public class BrowseIndexesScreenViewModel : Conductor<Index>.Collection.OneActive, IRavenScreen
    {
        private bool isBusy;

        public BrowseIndexesScreenViewModel(IDatabase database)
        {
            this.DisplayName = "Browse Indexes";
            this.Database = database;

            this.AllItems = new BindableCollection<Index>();

            CompositionInitializer.SatisfyImports(this);
        }

        public IDatabase Database { get; set; }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public bool IsBusy
        {
            get { return this.isBusy; }
            set
            {
                this.isBusy = value;
                this.NotifyOfPropertyChange(() => this.IsBusy);
            }
        }

        [Import]
        public IWindowManager WindowManager { get; set; }

        public IObservableCollection<Index> AllItems { get; set; }

        #region IRavenScreen Members

        public IRavenScreen ParentRavenScreen { get; set; }

        #endregion

        protected override void OnInitialize()
        {
            base.OnInitialize();
            this.isBusy = true;

            this.Database.IndexSession.LoadMany((result) =>
                                                    {
                                                        if (result.IsSuccess)
                                                        {
                                                            List<Index> list = result.Data.Select(index => new Index(index)).ToList();
                                                            this.AllItems.AddRange(list);
                                                            this.Items.AddRange(list);
                                                        }

                                                        this.IsBusy = false;
                                                    });
        }

        public void Remove(Index index)
        {
            this.IsBusy = true;
            this.Database.IndexSession.Delete(index.Name, (result) =>
                                                              {
                                                                  if (result.IsSuccess)
                                                                  {
                                                                      this.Items.Remove(index);
                                                                  }
                                                                  else
                                                                  {
                                                                      this.WindowManager.ShowDialog(new InformationDialogViewModel("Error", result.Exception.Message));
                                                                  }

                                                                  this.IsBusy = false;
                                                              });
        }

        public void Search(string text)
        {
            text = text.Trim();
            this.Items.Clear();

            this.Items.AddRange(!string.IsNullOrEmpty(text) ? this.AllItems.Where(x => x.Name.ToLowerInvariant().Contains(text.ToLowerInvariant())) : this.AllItems);
        }

        public void Save(Index index)
        {
            this.IsBusy = true;
            this.Database.IndexSession.Save(new KeyValuePair<string, IndexDefinition>(index.Name, index.Definition), (result) =>
                                                                                                                         {
                                                                                                                             if (result.IsSuccess)
                                                                                                                             {
                                                                                                                                 index.IsEdited = false;
                                                                                                                                 if (index.CurrentName != result.Data.Key)
                                                                                                                                 {
                                                                                                                                     var newIndex = new Index(result.Data);
                                                                                                                                     this.AllItems.Add(newIndex);
                                                                                                                                     this.Items.Add(newIndex);

                                                                                                                                     index.Name = index.CurrentName;
                                                                                                                                 }
                                                                                                                             }
                                                                                                                             else
                                                                                                                             {
                                                                                                                                 this.WindowManager.ShowDialog(new InformationDialogViewModel("Error", result.Exception.Message));
                                                                                                                             }

                                                                                                                             this.IsBusy = false;
                                                                                                                         });
        }

        public void AddFieldStorageAndIndexing(Index index)
        {
            index.AddFieldStorageAndIndexing();
        }
    }
}