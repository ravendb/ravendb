namespace Raven.Studio.Documents
{
	using System;
	using System.ComponentModel.Composition;
	using Database;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Plugins;
	using Raven.Database;

	[Export(typeof (IPlugin))]
	public class CreateNewDocumentPlugin : PluginBase
	{
		public override SectionType Section
		{
			get { return SectionType.Documents; }
		}

		public override IRavenScreen RelatedScreen
		{
			get
			{
				throw new NotImplementedException("This class is likely going away");
				//return new DocumentViewModel(new JsonDocument
				//                                            {
				//                                                DataAsJson = new JObject(),
				//                                                Metadata = new JObject()
				//                                            }, Server);
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