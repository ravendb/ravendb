// -----------------------------------------------------------------------
//  <copyright file="Posts_ByContent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Utils.Indexes
{
	public class Posts_ByContent : AbstractIndexCreationTask<Post>
	{
		public Posts_ByContent()
		{
			Map = posts => from post in posts
						   let body = LoadDocument<PostContent>(post.Id + "/content")
						   select new
						   {
							   Text = body == null ? null : body.Text
						   };
		}
	}
}