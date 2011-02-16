namespace Raven.Studio.Messages
{
	using Features.Database;

	public class DocumentDeleted : NotificationRaised
	{
		readonly DocumentViewModel document;

		public DocumentDeleted(DocumentViewModel document)
			: base("Document Deleted", NotificationLevel.Info)
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