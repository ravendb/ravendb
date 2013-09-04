using Raven.Abstractions.Data;

namespace Raven.Database.Data
{
	public class TouchedDocumentInfo
	{
		public Etag TouchedEtag { get; set; }
		public Etag PreTouchEtag { get; set; }
	}
}