//-----------------------------------------------------------------------
// <copyright file="QueryWithPercentageSign.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryWithPercentageSign : RavenTest
	{
		[Fact]
		public void CanQueryUsingPercentageSign()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Tags/Count",
					new IndexDefinition
					{
						Map = "from tag in docs.Tags select new { tag.Name, tag.UserId }"
					});

				using (var session = store.OpenSession())
				{
					var userId = "users/1";
					var tag = "24%";
// ReSharper disable once ReturnValueOfPureMethodIsNotUsed
					session.Query<TagCount>("Tags/Count").FirstOrDefault(x => x.Name == tag && x.UserId == userId);
				}
			}
		}

		public class TagCount
		{
			public string Name { get; set; }
			public string UserId { get; set; }
		}
	}
}