namespace Raven.Studio.Features.Documents
{
	using System.Windows.Controls;
	using Caliburn.Micro;

	public class DocumentTemplateSelector : ContentControl
	{
		static readonly IDocumentTemplateProvider Templates;

		static DocumentTemplateSelector()
		{
			if(!Bootstrapper.IsInDesignMode)
				Templates = IoC.Get<IDocumentTemplateProvider>();
		}

		protected override void OnContentChanged(object oldContent, object newContent)
		{
			var doc = newContent as ISupportDocumentTemplate;
			if (doc == null) return;

			// first, check the cache
			var cached = Templates.RetrieveFromCache(doc.TemplateKey);
			if(cached != null)
			{
				ContentTemplate = cached;
				return;
			}

			// second, if we don't have a cached template, then use the default as a temporary while we check the database
			ContentTemplate = Templates.GetDefaultTemplate(doc.TemplateKey);

			// finally, request the template from the database
			Templates
				.GetTemplateFor(doc.TemplateKey)
				.ContinueWith(x => Execute.OnUIThread(() => ContentTemplate = x.Result));

			base.OnContentChanged(oldContent, newContent);
		}
	}
}