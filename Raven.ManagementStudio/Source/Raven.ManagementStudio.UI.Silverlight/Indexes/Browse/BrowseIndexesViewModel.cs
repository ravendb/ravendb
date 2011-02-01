namespace Raven.ManagementStudio.UI.Silverlight.Indexes.Browse
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Messages;
	using Models;
	using Plugin;

	public class BrowseIndexesViewModel : Conductor<IndexViewModel>.Collection.OneActive, IRavenScreen,
	                                      IHandle<IndexChangeMessage>
	{
		const string WatermarkFilterString = "search by index name";
		string filter;

		bool isBusy;

		public BrowseIndexesViewModel(IDatabase database)
		{
			DisplayName = "Browse Indexes";
			Database = database;

			AllItems = new BindableCollection<string>(new[]{"Hello","World"});

			CompositionInitializer.SatisfyImports(this);
		}

		public IDatabase Database { get; private set; }

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		[Import]
		public IWindowManager WindowManager { get; set; }

		public BindableCollection<string> AllItems { get; private set; }

		public string Filter
		{
			get { return filter; }
			set
			{
				if (filter != value)
				{
					filter = value;
					NotifyOfPropertyChange(() => Filter);
					Search(filter);
				}
			}
		}

		public void Handle(IndexChangeMessage message)
		{
			//IndexViewModel index = message.Index;

			//if (index.Database == Database)
			//{
			//    if (message.IsRemoved)
			//    {
			//        AllItems.Remove(index);
			//        Items.Remove(index);
			//    }
			//    else
			//    {
			//        AllItems.Add(index);
			//        Items.Add(index);
			//    }
			//}
		}

		public IRavenScreen ParentRavenScreen { get; set; }

		public SectionType Section
		{
			get { return SectionType.Indexes; }
		}

		protected override void OnInitialize()
		{
			IsBusy = true;

			Database.Session.Advanced.AsyncDatabaseCommands
			.GetIndexNamesAsync(0, 25)
			.ContinueWith(x =>
							{
								AllItems = new BindableCollection<string>(x.Result);
								NotifyOfPropertyChange(() => AllItems);
								IsBusy = false;
							});

		}

		public void Search(string text)
		{
			//text = text.Trim();
			//Items.Clear();

			//Items.AddRange(!string.IsNullOrEmpty(text) && text != WatermarkFilterString
			//                ? AllItems.Where(item => item.IndexOf(text, StringComparison.InvariantCultureIgnoreCase) >= 0)
			//                : AllItems);
		}
	}
}