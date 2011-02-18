namespace Raven.Studio.Messages
{
	public class CollectionTemplateUpdated : NotificationRaised
	{
		readonly string templateKey;
		readonly string xaml;

		public CollectionTemplateUpdated(string templateKey, string xaml)
			: base("Template Saved", NotificationLevel.Info)
		{
			this.templateKey = templateKey;
			this.xaml = xaml;
		}

		public string TemplateKey
		{
			get { return templateKey; }
		}

		public string Xaml
		{
			get { return xaml; }
		}
	}
}