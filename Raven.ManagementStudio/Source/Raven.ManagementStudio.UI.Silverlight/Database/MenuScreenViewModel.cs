namespace Raven.ManagementStudio.UI.Silverlight.Database
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Plugin;

	public class MenuScreenViewModel : Screen, IRavenScreen
	{
		IEnumerable<IPlugin> administrationPlugins;
		IEnumerable<IPlugin> collectionPlugins;
		IEnumerable<IPlugin> currentPlugins;
		SectionType currentSectionType;
		IEnumerable<IPlugin> documentPlugins;
		IEnumerable<IPlugin> indexPlugins;
		IEnumerable<IPlugin> otherPlugins;
		IEnumerable<IPlugin> plugins;
		IEnumerable<IPlugin> statisticPlugins;
		bool isBusy;

		public MenuScreenViewModel(IDatabase database)
		{
			Database = database;
			DisplayName = "Home";
			CompositionInitializer.SatisfyImports(this);
		}

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
		public IEventAggregator EventAggregator { get; set; }

		[ImportMany(AllowRecomposition = true)]
		public IEnumerable<IPlugin> Plugins
		{
			get { return plugins; }
			set
			{
				plugins = value;

				foreach (var plugin in plugins)
				{
					plugin.Database = Database;
				}

				NotifyOfPropertyChange(() => Plugins);
				NotifyOfPropertyChange(() => DocumentPlugins);
				NotifyOfPropertyChange(() => CollectionPlugins);
				NotifyOfPropertyChange(() => IndexPlugins);
				NotifyOfPropertyChange(() => StatisticPlugins);
				NotifyOfPropertyChange(() => AdministrationPlugins);
				NotifyOfPropertyChange(() => OtherPlugins);
				SetCurrentPlugins();
			}
		}

		public IEnumerable<IPlugin> CurrentPlugins
		{
			get { return currentPlugins; }
			private set
			{
				currentPlugins = value;
				NotifyOfPropertyChange(() => CurrentPlugins);
			}
		}

		public IEnumerable<IPlugin> DocumentPlugins
		{
			get { return documentPlugins ?? (documentPlugins = Plugins.Where(x => x.Section == SectionType.Documents)); }
		}

		public IEnumerable<IPlugin> CollectionPlugins
		{
			get { return collectionPlugins ?? (collectionPlugins = Plugins.Where(x => x.Section == SectionType.Collections)); }
		}

		public IEnumerable<IPlugin> IndexPlugins
		{
			get { return indexPlugins ?? (indexPlugins = Plugins.Where(x => x.Section == SectionType.Indexes)); }
		}

		public IEnumerable<IPlugin> StatisticPlugins
		{
			get { return statisticPlugins ?? (statisticPlugins = Plugins.Where(x => x.Section == SectionType.Statistics)); }
		}

		public IEnumerable<IPlugin> AdministrationPlugins
		{
			get
			{
				return administrationPlugins ??
				       (administrationPlugins = Plugins.Where(x => x.Section == SectionType.Administration));
			}
		}

		public IEnumerable<IPlugin> OtherPlugins
		{
			get { return otherPlugins ?? (otherPlugins = Plugins.Where(x => x.Section == SectionType.Other)); }
		}

		public IDatabase Database { get; set; }

		public SectionType CurrentSectionType
		{
			get { return currentSectionType; }
			set
			{
				currentSectionType = value;
				SetCurrentPlugins();
			}
		}

		public IRavenScreen ParentRavenScreen { get; set; }

		public SectionType Section
		{
			get { return SectionType.None; }
		}

		void SetCurrentPlugins()
		{
			CurrentPlugins = Plugins
				.Where(p => p.Section == CurrentSectionType)
				.OrderBy(p => p.Ordinal)
				.ToArray();
		}

		public void Activate(IRavenScreen screen)
		{
			if (currentPlugins == null) return;

			foreach (var plugin in currentPlugins)
			{
				plugin.IsActive = plugin.RelatedScreen.GetType() == screen.GetType();
			}
		}
	}
}