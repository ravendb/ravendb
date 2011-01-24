//-----------------------------------------------------------------------
// <copyright file="Post.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Sample.ComplexSharding
{
	public class Post
	{
		public string Id { get; set; }
		public string Title { get; set; }
		public string Content { get; set; }
		public string BlogId { get; set; }
		public string UserId { get; set; }
	}
}
