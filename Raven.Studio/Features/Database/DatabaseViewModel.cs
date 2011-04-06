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

	[Export(typeof(DatabaseViewModel))]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class DatabaseViewModel : Conductor<IScreen>,
		IHandle<DatabaseScreenRequested>,
		IHandle<DocumentDeleted>
	{
		readonly IEventAggregator events;
		readonly IList<Lazy<IDatabaseScreenMenuItem, IMenuItemMetadata>> screens;
		readonly IServer server;

		[ImportingConstructor]
		public DatabaseViewModel(IServer server, IEventAggregator events, [ImportMany]IEnumerable<Lazy<IDatabaseScreenMenuItem, IMenuItemMetadata>> screens)
		{
			this.server = server;
			this.events = events;
			this.screens = screens.OrderBy(x => x.Metadata.Index).ToList();
			DisplayName = "DATABASE";

			Items = this.screens.Select(x => x.Metadata.DisplayName).ToList();

			SelectedItem = this.screens.Select(x => x.Metadata.DisplayName).First();

			events.Subscribe(this);
		}

		public IList<string> Items { get; private set; }

		string selectedItem;
		public string SelectedItem
		{
			get { return selectedItem; }
			set
			{
				selectedItem = value;
				NotifyOfPropertyChange(() => SelectedItem);
				ActivateItem(screens.First(x => x.Metadata.DisplayName == selectedItem).Value);
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
				ActiveItem = screens.Skip(3).Select(x => x.Value).First();
				doc.TryClose();

			}
		}
	}
}