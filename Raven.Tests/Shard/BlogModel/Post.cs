using System;

namespace Raven.Tests.Shard.BlogModel
{
	public class Post
	{
		public string Id { get; set; }
		public string Title { get; set; }
		public string Content { get; set; }
		public string BlogId { get; set; }
		public string UserId { get; set; }
		public int VotesUpCount { get; set; }
		public DateTime PublishAt { get; set; }
	}
}