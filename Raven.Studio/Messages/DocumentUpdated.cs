namespace Raven.Studio.Messages
{
	using Features.Database;
	using Features.Documents;

	public class DocumentUpdated : NotificationRaised
	{
		readonly EditDocumentViewModel document;

		public DocumentUpdated(EditDocumentViewModel document) : base( "Document Saved", NotificationLevel.Info)
		{
			this.document = document;
		}

		public string DocumentId
		{
			get { return document.Id; }
		}

		public EditDocumentViewModel Document
		{
			get { return document; }
		}
	}
}