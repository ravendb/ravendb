namespace Raven.ManagementStudio.UI.Silverlight.Plugins
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Plugin;

	public abstract class PluginBase : PropertyChangedBase, IPlugin
	{
		bool isActive;

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		public abstract SectionType Section { get; }

		public abstract IRavenScreen RelatedScreen { get; }

		public abstract string Name { get; }

		public virtual object MenuView
		{
			get { return null; }
		}

		public virtual int Ordinal
		{
			get { return 0; }
		}

		public IDatabase Database { get; set; }

		public void GoToScreen()
		{
			EventAggregator.Publish(new OpenNewScreen(RelatedScreen));
		}

		public bool IsActive
		{
			get { return isActive; }
			set
			{
				isActive = value;
				NotifyOfPropertyChange(() => IsActive);
			}
		}
	}
}