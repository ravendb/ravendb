// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------
namespace Raven.Tests.Core.Utils.Entities
{
    public class Post
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Desc { get; set; }
        public Post[] Comments { get; set; }
		public string[] AttachmentIds { get; set; }
    }
}
