namespace Raven.Studio.Features.Documents
{
	using System.Windows.Controls;
	using Caliburn.Micro;

	public class DocumentTemplateSelector : ContentControl
	{
		static readonly IDocumentTemplateProvider Templates;

		static DocumentTemplateSelector()
		{
			Templates = IoC.Get<IDocumentTemplateProvider>();
		}

		protected override void OnContentChanged(object oldContent, object newContent)
		{
			var doc = newContent as ISupportDocumentTemplate;
			if (doc == null) return;

			var cached = Templates.RetrieveFromCache(doc.TemplateKey);
			if(cached != null)
			{
				ContentTemplate = cached;
				return;
			}

			var template = Templates.GetTemplateFor(doc.TemplateKey);
			template.ContinueWith(x => Execute.OnUIThread(() => ContentTemplate = x.Result));

			base.OnContentChanged(oldContent, newContent);
		}
	}
}