// -----------------------------------------------------------------------
//  <copyright file="PostWithAttachmentsContentTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Utils.Transformers
{
	public class PostWithMetadataForTransformer : AbstractTransformerCreationTask<Post>
	{
		public class Result
		{
			public string Title { get; set; }
			public DateTime LastModified { get; set; }
		}

		public PostWithMetadataForTransformer()
		{
			TransformResults = posts => from post in posts
										select new Result
										{
											Title = post.Title,
											LastModified = MetadataFor(post).Value<DateTime>("Last-Modified")
										};
		}
	}
}