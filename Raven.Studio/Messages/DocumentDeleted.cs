namespace Raven.Studio.Messages
{
	public class DocumentDeleted
	{
		public string DocumentId { get; private set; }

		public DocumentDeleted(string documentId)
		{
			DocumentId = documentId;
		}
	}
}