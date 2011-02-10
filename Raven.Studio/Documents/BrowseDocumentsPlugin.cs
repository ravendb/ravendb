namespace Raven.Studio.Documents
{
	using System;
	using System.ComponentModel.Composition;
	using Plugin;
	using Plugins;

	[Export(typeof (IPlugin))]
	public class BrowseDocumentsPlugin : PluginBase
	{
		public override string Name
		{
			get { return "BROWSE"; }
		}

		public override SectionType Section
		{
			get { return SectionType.Documents; }
		}

		public override IRavenScreen RelatedScreen
		{
			get
			{
				throw new NotImplementedException();
				//return new BrowseDocumentsViewModel(Server);
			}
		}

		public override object MenuView
		{
			get { return new BrowseMenuIcon(); }
		}
	}
}