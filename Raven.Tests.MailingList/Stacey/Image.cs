using System.Collections.Generic;

namespace Raven.Tests.MailingList.Stacey
{
	public class Image
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public ICollection<string> Users { get; set; }
		public ICollection<string> Tags { get; set; }
	}
}