// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;

namespace FastTests.Server.Basic.Entities
{
    public class Post
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Desc { get; set; }
        public Post[] Comments { get; set; }
        public string[] AttachmentIds { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
