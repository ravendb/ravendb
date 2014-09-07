// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2101.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Queries;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2101 : RavenTest
	{
		[Fact]
		public void SelectPropertiesShouldBeInTheSameLineLikeSelectStatement()
		{
			using (var store = NewDocumentStore())
			{
				store.SystemDatabase.ExecuteDynamicQuery("Employees", new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "LastName:James"
				}, CancellationToken.None);

				var indexDefinition = store.DatabaseCommands.GetIndex("Auto/Employees/ByLastName");

				Assert.Equal(
"from doc in docs.Employees\nselect new {\n\tLastName = doc.LastName\n}",
indexDefinition.Map);
				
			}
		}


		[Fact]
		public void ShouldGenerateLineBreaksForDynamicIndexDefinition()
		{
			using (var store = NewDocumentStore())
			{
				store.SystemDatabase.ExecuteDynamicQuery("Blogs", new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Title:RavenDB AND Category:Rhinos"
				}, CancellationToken.None);

				var indexDefinition = store.DatabaseCommands.GetIndex("Auto/Blogs/ByCategoryAndTitle");

				Assert.Equal(
"from doc in docs.Blogs\nselect new {\n\tCategory = doc.Category,\n\tTitle = doc.Title\n}", indexDefinition.Map);

				store.SystemDatabase.ExecuteDynamicQuery("Users", new IndexQuery()
				{
					PageSize = 128,
					Start = 0,
					Cutoff = SystemTime.UtcNow,
					Query = "Friends,Name: Arek"
				}, CancellationToken.None);

				indexDefinition = store.DatabaseCommands.GetIndex("Auto/Users/ByFriends_Name");

				Assert.Equal(
"from doc in docs.Users\nselect new {\n\tFriends_Name = (\n\t\tfrom docFriendsItem in ((IEnumerable<dynamic>)doc.Friends).DefaultIfEmpty()\n\t\tselect docFriendsItem.Name).ToArray()\n}"
, indexDefinition.Map);

			}
		}
	}
}