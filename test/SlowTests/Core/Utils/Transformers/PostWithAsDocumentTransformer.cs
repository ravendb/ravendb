// -----------------------------------------------------------------------
//  <copyright file="PostWithAsDocumentTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Post = SlowTests.Core.Utils.Entities.Post;

namespace SlowTests.Core.Utils.Transformers
{
    public class PostWithAsDocumentTransformer : AbstractTransformerCreationTask<Post>
    {
        public class Result
        {
            public JsonObject RawDocument { get; set; }
        }

        public PostWithAsDocumentTransformer()
        {
            TransformResults = posts => from post in posts
                                        select new Result
                                        {
                                            RawDocument = AsJson(post)
                                        };
        }
    }
}
