using System.ComponentModel.Composition;
using Raven.Studio.Commands;

namespace Raven.Studio.Infrastructure.Navigation
{
	[ExportMetadata("Url", @"docs/(.*)")]
	[Export(typeof(INavigator))]
	public class DocumentByIdNavigator : INavigator
	{
		private readonly EditDocumentById editDocumentById;

		[ImportingConstructor]
		public DocumentByIdNavigator(
			EditDocumentById editDocumentById)
		{
			this.editDocumentById = editDocumentById;
		}

		public void Navigate(string documentId)
		{
			editDocumentById.Execute(documentId);
		}
	}
}