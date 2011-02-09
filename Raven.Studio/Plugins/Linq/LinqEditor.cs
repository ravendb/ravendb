namespace Raven.Studio.Plugins.Linq
{
	using System.ComponentModel.Composition;
	using Plugin;

	[Export(typeof (IPlugin))]
	public class LinqEditor : PluginBase
	{
		public override SectionType Section
		{
			get { return SectionType.Linq; }
		}

		public override IRavenScreen RelatedScreen
		{
			get { return new LinqEditorViewModel(Server); }
		}

		public override string Name
		{
			get { return "EDITOR"; }
		}
	}
}