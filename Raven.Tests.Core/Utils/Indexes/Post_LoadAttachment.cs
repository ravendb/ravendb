// -----------------------------------------------------------------------
//  <copyright file="Post_LoadAttachment.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Utils.Indexes
{
	public class Post_LoadAttachment : AbstractIndexCreationTask<Post>
	{

		public Post_LoadAttachment()
		{
			Map = posts =>
				from post in posts
				from attachmentId in post.AttachmentIds
				select new
				{
					AttachmentContent = LoadAttachmentForIndexing(attachmentId)

				};

			Index("AttachmentContent", FieldIndexing.Analyzed);
		}
	}
}