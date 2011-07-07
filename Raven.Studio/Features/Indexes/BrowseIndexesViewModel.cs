using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Studio.Infrastructure.Navigation;

namespace Raven.Studio.Features.Indexes
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Framework;
	using Framework.Extensions;
	using Messages;
	using Plugins;
	using Plugins.Database;

	[Export]
	[ExportDatabaseExplorerItem(DisplayName = "Indexes", Index = 30)]
	public class BrowseIndexesViewModel : RavenScreen,
										  IHandle<IndexUpdated>
	{
		IndexDefinition activeIndex;
		object activeItem;
		private System.Action executeAfterIndexesFetched;

		[ImportingConstructor]
		public BrowseIndexesViewModel()
		{
			DisplayName = "Indexes";

			Events.Subscribe(this);
			Server.CurrentDatabaseChanged += delegate
			{
			    ActiveItem = null;
				if(Indexes != null) Indexes.Clear();
			};
		}

		protected override void OnInitialize()
		{
			Indexes = new BindablePagedQuery<IndexDefinition>((start, pageSize) =>
			{
				using(var session = Server.OpenSession())
				return session.Advanced.AsyncDatabaseCommands
					.GetIndexesAsync(start, pageSize);
			});
		}

		protected override void OnActivate()
		{
			BeginRefreshIndexes();
		}

		public void CreateNewIndex()
		{
			ActiveItem = new EditIndexViewModel(new IndexDefinition());
		}

		void BeginRefreshIndexes()
		{
			WorkStarted("retrieving indexes");
			using (var session = Server.OpenSession())
				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueWith(
						_ =>
							{
								WorkCompleted("retrieving indexes");
								RefreshIndexes(_.Result.CountOfIndexes);
							},
						faulted =>
							{
								WorkCompleted("retrieving indexes");
							}
						);
		}

		public BindablePagedQuery<IndexDefinition> Indexes { get; private set; }

		public IndexDefinition ActiveIndex
		{
			get { return activeIndex; }
			set
			{
				activeIndex = value;
				if (activeIndex != null)
					ActiveItem = new EditIndexViewModel(activeIndex);
				NotifyOfPropertyChange(() => ActiveIndex);
			}
		}

		public object ActiveItem
		{
			get
			{
				return activeItem;
			}
			set
			{
				var deactivatable = activeItem as IDeactivate;
				if (deactivatable != null) deactivatable.Deactivate(close:true);

				var activatable = value as IActivate;
				if (activatable != null) activatable.Activate();

				activeItem = value; 
				NotifyOfPropertyChange(() => ActiveItem);
			}
		}

		void IHandle<IndexUpdated>.Handle(IndexUpdated message)
		{
			BeginRefreshIndexes();

			if (message.IsRemoved)
			{
				ActiveItem = null;
			}
		}

		void RefreshIndexes(int totalIndexCount)
		{
			Indexes.GetTotalResults = () => totalIndexCount;
			Indexes.LoadPage()
				.ContinueOnSuccess(x =>
				                   	{
				                   		if (HasIndexes && executeAfterIndexesFetched != null)
				                   		{
				                   			executeAfterIndexesFetched();
				                   			executeAfterIndexesFetched = null;
				                   		}
				                   	});
		}

		public bool HasIndexes
		{
			get { return Indexes != null && Indexes.Any(); }
		}

		public void SelectIndexByName(string name)
		{
			if (HasIndexes)
			{
				var navigateTo = Indexes
					.Where(item => item.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase))
					.FirstOrDefault();

				if (navigateTo == null)
					return;

				ActiveIndex = navigateTo;
				return;
			}
			executeAfterIndexesFetched = () => SelectIndexByName(name);
		}
	}
}