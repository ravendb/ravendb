// -----------------------------------------------------------------------
//  <copyright file="PostWithAsDocumentTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Post = SlowTests.Core.Utils.Entities.Post;

namespace SlowTests.Core.Utils.Transformers
{
    public class PostWithAsDocumentTransformer : AbstractTransformerCreationTask<Post>
    {
        public class Result
        {
            //public RavenJObject RawDocument { get; set; }
        }

        public PostWithAsDocumentTransformer()
        {
            throw new NotImplementedException();
            //TransformResults = posts => from post in posts
            //                            select new Result
            //                            {
            //                                RawDocument = AsDocument(post)
            //                            };
        }
    }
}
