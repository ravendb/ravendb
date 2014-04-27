// -----------------------------------------------------------------------
//  <copyright file="PostWithContentTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Utils.Transformers
{
	public class PostWithContentTransformer : AbstractTransformerCreationTask<Post>
	{
		public class Result : Post
		{
			public string Content { get; set; }
		}

		public PostWithContentTransformer()
		{
			TransformResults = posts => from post in posts
										let content = LoadDocument<PostContent>(post.Id + "/content")
										select new Result
										{
											Id = post.Id,
											Comments = post.Comments,
											Desc = post.Desc,
											Title = post.Title,
											Content = content.Text
										};
		}
	}
}