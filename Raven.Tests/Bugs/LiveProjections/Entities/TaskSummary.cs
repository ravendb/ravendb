using System;

namespace Raven.Tests.Bugs.LiveProjections.Entities
{
	public class TaskSummary
	{
		public string Id { get; set; }

		public string Description { get; set; }

		public DateTime Start { get; set; }

		public DateTime End { get; set; }

		public int GiverId { get; set; }

		public string GiverName { get; set; }

		public int TakerId { get; set; }

		public string TakerName { get; set; }

		public int PlaceId { get; set; }

		public string PlaceName { get; set; }
	}
}