using System;
using System.ComponentModel.Composition;
using System.Linq;
using Caliburn.Micro;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Messages;
using Raven.ManagementStudio.UI.Silverlight.Models;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Indexes.Browse
{
    public class BrowseIndexesScreenViewModel : Conductor<IndexViewModel>.Collection.OneActive, IRavenScreen, IHandle<IndexChangeMessage>
    {
        private const string WatermarkFilterString = "search by index name";

        private bool _isBusy;

        public BrowseIndexesScreenViewModel(IDatabase database)
        {
            DisplayName = "Browse Indexes";
            Database = database;

            AllItems = new BindableCollection<IndexViewModel>();

            CompositionInitializer.SatisfyImports(this);
        }

        public IDatabase Database { get; set; }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

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
        public IWindowManager WindowManager { get; set; }

        public IObservableCollection<IndexViewModel> AllItems { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public SectionType Section { get { return SectionType.Indexes; } }

        private string _filter;

        public string Filter
        {
            get { return _filter; }
            set
            {
                if (_filter != value)
                {
                    _filter = value;
                    NotifyOfPropertyChange(() => Filter);
                    Search(_filter);
                }
            }
        }


        protected override void OnInitialize()
        {
            base.OnInitialize();
            IsBusy = true;
			throw new NotImplementedException();
			//Database.IndexSession.LoadMany(result =>
			//                                   {
			//                                       if (result.IsSuccess)
			//                                       {
			//                                           var list =
			//                                               result.Data.Select(index => new IndexViewModel(new Index(index.Key, index.Value), Database, this))
			//                                                   .ToList();
			//                                           AllItems.AddRange(list);
			//                                           Items.AddRange(list);
			//                                       }

			//                                       IsBusy = false;
			//                                   });
        }

        public void Search(string text)
        {
            text = text.Trim();
            Items.Clear();

            Items.AddRange(!string.IsNullOrEmpty(text) && text != WatermarkFilterString ? AllItems.Where(item => item.Name.IndexOf(text, StringComparison.InvariantCultureIgnoreCase) >= 0) : AllItems);
        }

        public void Handle(IndexChangeMessage message)
        {
            IndexViewModel index = message.Index;

            if (index.Database == Database)
            {
                if (message.IsRemoved)
                {
                    AllItems.Remove(index);
                    Items.Remove(index);
                }
                else
                {
                    AllItems.Add(index);
                    Items.Add(index);
                }
            }
        }
    }
}
