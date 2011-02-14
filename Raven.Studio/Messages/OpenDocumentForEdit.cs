namespace Raven.Studio.Messages
{
	using Features.Database;

	public class OpenDocumentForEdit
	{
		public OpenDocumentForEdit(DocumentViewModel document)
		{
			Document = document;
		}

		public DocumentViewModel Document { get; private set; }
	}
}