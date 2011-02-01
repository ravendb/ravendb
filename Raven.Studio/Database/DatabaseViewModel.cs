namespace Raven.Studio.Database
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Messages;
	using Plugin;

	public class DatabaseViewModel : Conductor<IRavenScreen>.Collection.OneActive, IHandle<ReplaceActiveScreen>
	{
		public DatabaseViewModel(IDatabase database)
		{
			Database = database;
			DisplayName = Database.Name;
			Menu = new MenuScreenViewModel(Database);
			Home = new HomeScreenViewModel(Database);
			GoHome();
			CompositionInitializer.SatisfyImports(this);
			EventAggregator.Subscribe(this);
		}

		public MenuScreenViewModel Menu { get; private set; }
		public HomeScreenViewModel Home { get; set; }

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		public IDatabase Database { get; private set; }

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