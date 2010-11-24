namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Linq;
    using System.Windows;
    using System.Windows.Controls;
    using Caliburn.Micro;
    using Raven.ManagementStudio.Plugin;
    using Raven.ManagementStudio.UI.Silverlight.Messages;
    using Raven.ManagementStudio.UI.Silverlight.Models;
    using Raven.ManagementStudio.UI.Silverlight.ViewModels;

    public class BrowseIndexesScreenViewModel : Conductor<Index>.Collection.OneActive, IRavenScreen
    {
        private bool isBusy;
        //private Index selectedItem;

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

        //public Index SelectedItem
        //{
        //    get { return this.selectedItem; }
        //    set
        //    {
        //        this.selectedItem = value;
        //        this.NotifyOfPropertyChange(() => this.SelectedItem);
        //    }
        //}

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

        //public void ToogleEdit(Index index)
        //{         
        //    index.IsEdited = !index.IsEdited;

        //    var x = this.Items.IndexOf(index);
        //    this.Items.Remove(index);
        //    this.Items.Insert(x, index);

        //    this.SelectedItem = index;
        //}

        public void Edit(Index index)
        {
            this.WindowManager.ShowDialog(new IndexFormViewModel(this.Database, index));
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
                                                                      this.WindowManager.ShowDialog(new ErrorViewModel(result.Exception.Message));
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

        //public void Cancel()
        //{
        //    var view = this.GetView(null) as Control;
        //    if (view != null)
        //    {
        //        VisualStateManager.GoToState(view, "NormalState", false);
        //    }
        //    else
        //    {
        //        this.EventAggregator.Publish(new ReplaceActiveScreen(this));
        //        view = this.GetView(null) as Control;
        //        if (view != null)
        //        {
        //            VisualStateManager.GoToState(view, "NormalState", false);
        //        }
        //    }
        //}
    }
}