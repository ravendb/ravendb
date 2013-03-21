using System;

namespace Raven.Tests.Bugs.LiveProjections.Entities
{
	public class Task
	{
		public long Id { get; set; }

		public string Description { get; set; }

		public DateTime Start { get; set; }

		public DateTime End { get; set; }

		public int GiverId { get; set; }

		public int TakerId { get; set; }

		public int PlaceId { get; set; }
	}
}