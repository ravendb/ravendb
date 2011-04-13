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

			events.Subscribe(this);
			server.CurrentDatabaseChanged += delegate
			{
				DisplayName = server.CurrentDatabase.ToUpper();
				                     		
			};
		}

		[ImportMany("Raven.DatabaseExplorerItem", AllowRecomposition = true)]
		public IEnumerable<Lazy<object, IMenuItemMetadata>> AvailableItems { get; set; }

		public IList<string> Items { get; private set; }

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

		public void ShowByDisplayName(string item)
		{
			if (!AvailableItems.Any()) return;
			var screen = (IScreen)AvailableItems.OrderBy(x => x.Metadata.Index).First(x => x.Metadata.DisplayName == item).Value;
			Show(screen);
		}

		void IHandle<DatabaseScreenRequested>.Handle(DatabaseScreenRequested message)
		{
			Show(message.GetScreen());
		}

		void IHandle<DocumentDeleted>.Handle(DocumentDeleted message)
		{
			var doc = ActiveItem as EditDocumentViewModel;
			if (doc != null && doc.Id == message.DocumentId)
			{
				//TODO: this is an arbitrary choice, we should actually go back using the history
				ActiveItem = AvailableItems.OrderBy(x => x.Metadata.Index).Skip(3).Select(x => x.Value).First();
				doc.TryClose();
			}
		}

		void IPartImportsSatisfiedNotification.OnImportsSatisfied()
		{
			Items = AvailableItems
				.OrderBy(x => x.Metadata.Index)
				.Select(x => x.Metadata.DisplayName)
				.ToList();
			
			ShowFirstScreen();
		}

		void ShowFirstScreen()
		{
			SelectedItem = Items.FirstOrDefault();
			var first = (IScreen)AvailableItems.First(_ => _.Metadata.DisplayName == SelectedItem).Value;
			Show(first);
		}
	}
}