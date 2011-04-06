namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Documents;
	using Framework;
	using Framework.Extensions;
	using Messages;
	using System.Linq;
	using Plugins;

	[Export(typeof(DatabaseExplorer))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class DatabaseExplorer : Conductor<object>, IPartImportsSatisfiedNotification,
		IHandle<DatabaseScreenRequested>,
		IHandle<DocumentDeleted>
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public DatabaseExplorer(IServer server, IEventAggregator events)
		{
			this.server = server;
			this.events = events;
			DisplayName = "DATABASE";

			events.Subscribe(this);
		}

		[ImportMany("Raven.DatabaseExplorerItem", AllowRecomposition = true)]
		public IEnumerable<Lazy<object, IMenuItemMetadata>> AvailableItems { get; set; }

		public IList<string> Items { get; private set; }

		public void NavigateTo(string item)
		{
			if (!AvailableItems.Any()) return;
			ActivateItem(AvailableItems.OrderBy(x => x.Metadata.Index).First(x => x.Metadata.DisplayName == item).Value);
		}

		string selectedItem;
		public string SelectedItem
		{
			get { return selectedItem; }
			set
			{
				selectedItem = value;
				NotifyOfPropertyChange(() => SelectedItem);
			}
		}

		public IServer Server
		{
			get { return server; }
		}

		public void Show(IScreen screen)
		{
			this.TrackNavigationTo(screen, events);
		}

		public void Handle(DatabaseScreenRequested message)
		{
			Show(message.GetScreen());
		}

		public void Handle(DocumentDeleted message)
		{
			var doc = ActiveItem as EditDocumentViewModel;
			if (doc != null && doc.Id == message.DocumentId)
			{
				//TODO: this is an arbitrary choice, we should actually go back using the history
				ActiveItem = AvailableItems.OrderBy(x => x.Metadata.Index).Skip(3).Select(x => x.Value).First();
				doc.TryClose();
			}
		}

		public void OnImportsSatisfied()
		{
			var screens = AvailableItems.OrderBy(x => x.Metadata.Index).ToList();

			Items = screens.Select(x => x.Metadata.DisplayName).ToList();
			SelectedItem = screens.Select(x => x.Metadata.DisplayName).FirstOrDefault();
			NavigateTo(SelectedItem);
		}
	}
}