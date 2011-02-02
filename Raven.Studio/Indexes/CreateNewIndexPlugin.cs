namespace Raven.Studio.Indexes
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Plugin;
	using Plugins;
	using Raven.Database.Indexing;

	[Export(typeof (IPlugin))]
	public class CreateNewIndexPlugin : PluginBase
	{
		public override string Name
		{
			get { return "CREATE NEW"; }
		}

		public override SectionType Section
		{
			get { return SectionType.Indexes; }
		}

		public override IRavenScreen RelatedScreen
		{
			get
			{
				return new EditIndexViewModel(new IndexDefinition
				                                                  	{
				                                                  		Analyzers = new Dictionary<string, string>(),
				                                                  		Indexes = new Dictionary<string, FieldIndexing>(),
				                                                  		SortOptions = new Dictionary<string, SortOptions>(),
				                                                  		Stores = new Dictionary<string, FieldStorage>(),
				                                                  	}, Database);
			}
		}

		public override object MenuView
		{
			get { return new BrowseIndexesMenuIcon(); }
		}

		public override int Ordinal
		{
			get { return 1; }
		}
	}
}