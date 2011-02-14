namespace Raven.Studio.Messages
{
	using Features.Database;

	public class DocumentDeleted
	{
		readonly DocumentViewModel document;

		public DocumentDeleted(DocumentViewModel document)
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