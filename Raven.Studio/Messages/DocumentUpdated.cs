namespace Raven.Studio.Messages
{
	using Features.Database;

	public class DocumentUpdated
	{
		readonly DocumentViewModel document;

		public DocumentUpdated(DocumentViewModel document)
		{
			this.document = document;
		}

		public string DocumentId
		{
			get { return document.Id; }
		}

		public DocumentViewModel Document
		{
			get { return document; }
		}
	}
}