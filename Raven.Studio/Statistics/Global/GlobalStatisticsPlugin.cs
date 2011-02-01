namespace Raven.Studio.Statistics.Global
{
	using System.ComponentModel.Composition;
	using Plugin;
	using Plugins;

	[Export(typeof (IPlugin))]
	public class GlobalStatisticsPlugin : PluginBase
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
			get { return new GlobalStatisticsViewModel(Database); }
		}

		public override object MenuView
		{
			get { return new GlobalStatisticsMenuIcon(); }
		}
	}
}