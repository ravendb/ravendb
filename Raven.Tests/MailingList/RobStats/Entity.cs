using System;

namespace Raven.Tests.MailingList.RobStats
{
	public class Entity
	{
		public string Id { get; set; }
		public string DisplayName { get; set; }
		public string Visibility { get; set; }
		public DateTimeOffset UpdatedAt { get; set; }
	}
}