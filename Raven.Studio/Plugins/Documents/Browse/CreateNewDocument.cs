namespace Raven.Studio.Plugins.Documents.Browse
{
	using System.ComponentModel.Composition;
	using Common;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Raven.Database;

	[Export(typeof (IPlugin))]
	public class CreateNewDocument : PluginBase
	{
		public override SectionType Section
		{
			get { return SectionType.Documents; }
		}

		public override IRavenScreen RelatedScreen
		{
			get
			{
				return new DocumentViewModel(new Document(new JsonDocument
				                                          	{
				                                          		DataAsJson = new JObject(),
				                                          		Metadata = new JObject()
				                                          	}), Database);
			}
		}

		public override object MenuView
		{
			get { return null; }
		}

		public override string Name
		{
			get { return "CREATE NEW"; }
		}

		public override int Ordinal
		{
			get { return 1; }
		}
	}
}