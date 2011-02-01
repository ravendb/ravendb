namespace Raven.ManagementStudio.UI.Silverlight.Indexes.Browse
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Models;
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
				return new IndexViewModel(new Index(string.Empty, new IndexDefinition
				                                                  	{
				                                                  		Analyzers = new Dictionary<string, string>(),
				                                                  		Indexes = new Dictionary<string, FieldIndexing>(),
				                                                  		SortOptions = new Dictionary<string, SortOptions>(),
				                                                  		Stores = new Dictionary<string, FieldStorage>(),
				                                                  	}), Database, null);
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