namespace Raven.Studio.Database
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Framework;
	using Messages;
	using Plugin;

	[Export(typeof(DatabaseViewModel))]
	public class DatabaseViewModel : Conductor<IRavenScreen>.Collection.OneActive, IHandle<ReplaceActiveScreen>
	{
		readonly IEventAggregator events;

		[ImportingConstructor]
		public DatabaseViewModel(IServer server, TemplateColorProvider colorProvider, IEventAggregator events)
		{
			this.events = events;
			DisplayName = server.Name;
			events.Subscribe(this);

			server.Connect(new Uri("http://localhost:8080"));

			Server = server;
			Menu = new MenuScreenViewModel(server);
			Home = new HomeScreenViewModel(server, events, colorProvider);
			GoHome();
		}

		public MenuScreenViewModel Menu { get; private set; }
		public HomeScreenViewModel Home { get; set; }

		public IServer Server { get; private set; }

		public void Handle(ReplaceActiveScreen message)
		{
			//if (message.NewScreen.ParentRavenScreen == ActiveItem)
			//{
			//    var index = Items.IndexOf(ActiveItem);
			//    CloseItem(ActiveItem);
			//    Ittems.Insert(index, message.NewScreen);
			//    ActiveItem = message.NewScreen;
			//}
			ActivateItem(message.NewScreen);
		}

		public void GoHome()
		{
			ViewPlugins(SectionType.None);
			ActivateItem(Home);
		}

		public void GoBack()
		{
			if (ActiveItem == null || Items.Count <= 1) return;

			Items.RemoveAt(Items.Count - 1);
			ActivateItem(Items.Last());
		}

		public void ViewPlugins(string sectionTypeString)
		{
			SectionType sectionType;
			Enum.TryParse(sectionTypeString, out sectionType);
			ViewPlugins(sectionType);
		}

		void ViewPlugins(SectionType sectionType)
		{
			Menu.CurrentSectionType = sectionType;
			IPlugin first = Menu.CurrentPlugins.FirstOrDefault();
			if (first != null)
			{
				first.GoToScreen();
			}
		}

		public override sealed void ActivateItem(IRavenScreen item)
		{
			base.ActivateItem(item);

			Menu.Activate(item);
		}
	}
}