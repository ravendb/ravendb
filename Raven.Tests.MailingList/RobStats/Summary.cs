using System;

namespace Raven.Tests.MailingList.RobStats
{
	public class Summary
	{
		public string EntityId { get; set; }
		public string DisplayName { get; set; }
		public string Visibility { get; set; }
		public DateTimeOffset UpdatedAt { get; set; }
		public int NumberOfFavorites { get; set; }
	}
}