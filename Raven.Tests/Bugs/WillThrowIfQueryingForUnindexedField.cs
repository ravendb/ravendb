//-----------------------------------------------------------------------
// <copyright file="WillThrowIfQueryingForUnindexedField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WillThrowIfQueryingForUnindexedField : RavenTest
	{
		[Fact]
		public void ThrowOnMapIndex()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from u in docs select new { u.Name }"
				});
				
				store.DatabaseCommands.Query("test", new IndexQuery
				{
					Query = "Name:Oren"
				}, new string[0]);

				var argumentException = Assert.Throws<ArgumentException>(() => store.DatabaseCommands.Query("test", new IndexQuery
				{
					Query = "User:Oren"
				}, new string[0]));

				Assert.Equal("The field 'User' is not indexed, cannot query on fields that are not indexed", argumentException.Message);
			}
		}

		[Fact]
		public void ThrowOnReduceIndex()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from u in docs select new { u.User }",
					Reduce = "from u in results group u by u.Name into g select new { User = g.Key }"
				});

				store.DatabaseCommands.Query("test", new IndexQuery
				{
					Query = "User:Oren"
				}, new string[0]);

				var argumentException = Assert.Throws<ArgumentException>(() => store.DatabaseCommands.Query("test", new IndexQuery
				{
					Query = "Name:Oren"
				}, new string[0]));

				Assert.Equal("The field 'Name' is not indexed, cannot query on fields that are not indexed", argumentException.Message);
			}
		}

		[Fact]
		public void ThrowOnSortIndex()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from u in docs select new { u.Name }",
				});

				var argumentException = Assert.Throws<ArgumentException>(() => store.DatabaseCommands.Query("test", new IndexQuery
				{
					Query = "Name:Oren",
					SortedFields = new[]{new SortedField("User"), }
				}, new string[0]));

				Assert.Equal("The field 'User' is not indexed, cannot sort on fields that are not indexed", argumentException.Message);
			}
		}
	}
}
