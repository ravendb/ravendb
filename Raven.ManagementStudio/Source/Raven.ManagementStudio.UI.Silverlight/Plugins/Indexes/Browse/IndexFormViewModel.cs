namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Raven.Database.Indexing;
    using Raven.ManagementStudio.Plugin;
    using Raven.ManagementStudio.UI.Silverlight.Models;
    using Raven.ManagementStudio.UI.Silverlight.ViewModels;

    public class IndexFormViewModel : Screen, IRavenScreen
    {
        private bool isBusy;

        public IndexFormViewModel(IDatabase database, Index index)
        {
            this.Database = database;
            this.DisplayName = index.Name;

            this.Indexes = new ObservableCollection<Index> {index};

            CompositionInitializer.SatisfyImports(this);
        }

        public IDatabase Database { get; set; }

        [Import]
        public IWindowManager WindowManager { get; set; }

        public bool IsBusy
        {
            get { return this.isBusy; }
            set
            {
                this.isBusy = value;
                this.NotifyOfPropertyChange(() => this.IsBusy);
            }
        }

        public ObservableCollection<Index> Indexes { get; set; }

        #region IRavenScreen Members

        public IRavenScreen ParentRavenScreen
        {
            get { throw new NotImplementedException(); }
        }

        #endregion

        public void Save(Index index)
        {
            this.IsBusy = true;
            this.Database.IndexSession.Save(new KeyValuePair<string, IndexDefinition>(index.Name, index.Definition), (result) =>
                                                                                                                         {
                                                                                                                             if (result.IsSuccess)
                                                                                                                             {
                                                                                                                                 this.TryClose();
                                                                                                                             }
                                                                                                                             else
                                                                                                                             {
                                                                                                                                 this.WindowManager.ShowDialog(new ErrorViewModel(result.Exception.Message));
                                                                                                                             }

                                                                                                                             this.IsBusy = false;
                                                                                                                         });
        }
    }
}