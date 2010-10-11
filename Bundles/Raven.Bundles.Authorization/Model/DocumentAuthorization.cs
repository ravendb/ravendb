using System.Collections.Generic;

namespace Raven.Bundles.Authorization.Model
{
	public class DocumentAuthorization
	{
		public List<string> Tags { get; set; }
		public List<DocumentPermission> Permissions { get; set; }

		public DocumentAuthorization()
		{
			Tags = new List<string>();
			Permissions = new List<DocumentPermission>();
		}
	}
}
