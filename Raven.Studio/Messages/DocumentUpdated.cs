namespace Raven.Studio.Messages
{
	using Features.Database;

	public class DocumentUpdated : NotificationRaised
	{
		readonly DocumentViewModel document;

		public DocumentUpdated(DocumentViewModel document) : base( "Document Saved", NotificationLevel.Info)
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