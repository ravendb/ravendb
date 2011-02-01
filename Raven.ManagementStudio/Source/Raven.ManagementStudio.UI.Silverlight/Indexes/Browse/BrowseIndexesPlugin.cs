namespace Raven.ManagementStudio.UI.Silverlight.Indexes.Browse
{
	using System.ComponentModel.Composition;
	using Plugin;
	using Plugins;

	[Export(typeof (IPlugin))]
	public class BrowseIndexesPlugin : PluginBase
	{
		public override string Name
		{
			get { return "BROWSE"; }
		}

		public override SectionType Section
		{
			get { return SectionType.Indexes; }
		}

		public override IRavenScreen RelatedScreen
		{
			get { return new BrowseIndexesViewModel(Database); }
		}

		public override object MenuView
		{
			get { return new BrowseIndexesMenuIcon(); }
		}
	}
}