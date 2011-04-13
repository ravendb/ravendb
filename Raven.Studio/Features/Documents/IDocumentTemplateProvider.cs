namespace Raven.Studio.Features.Documents
{
	using System.Threading.Tasks;
	using System.Windows;

	public interface IDocumentTemplateProvider
	{
		string GetDefaultTemplateXamlFor(string key);
		Task<DataTemplate> GetTemplateFor(string key);
		DataTemplate RetrieveFromCache(string key);
		DataTemplate GetDefaultTemplate(string key);
	}
}