namespace Raven.Studio.Messages
{
	public class DocumentDeleted : NotificationRaised
	{
		readonly string documentId;

		public DocumentDeleted(string documentId)
			: base("Document Deleted", NotificationLevel.Info)
		{
			this.documentId = documentId;
		}

		public string DocumentId
		{
			get { return documentId; }
		}
	}
}