// -----------------------------------------------------------------------
//  <copyright file="PostWithAttachmentsContentTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Post = SlowTests.Core.Utils.Entities.Post;

namespace SlowTests.Core.Utils.Transformers
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
                                            LastModified = MetadataFor(post).Value<DateTime>(Constants.Metadata.LastModified)
                                        };
        }
    }
}
