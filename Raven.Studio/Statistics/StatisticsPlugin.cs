namespace Raven.Studio.Statistics
{
	using System.ComponentModel.Composition;
	using Plugin;
	using Plugins;

	[Export(typeof (IPlugin))]
	public class StatisticsPlugin : PluginBase
	{
		public override string Name
		{
			get { return "GLOBAL"; }
		}

		public override SectionType Section
		{
			get { return SectionType.Statistics; }
		}

		public override IRavenScreen RelatedScreen
		{
			get { return new StatisticsViewModel(Server); }
		}

		public override object MenuView
		{
			get { return new StatisticsMenuIcon(); }
		}
	}
}