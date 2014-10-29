using System;
using System.Collections.Generic;

namespace Raven.Tests.Common.Dto.TagCloud
{
	public class Post
	{
		public string Id { get; set; }
		public string Title { get; set; }
		public DateTime PostedAt { get; set; }
		public List<string> Tags { get; set; }

		public string Content { get; set; }
	}
}