// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1345.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
	using Raven.Abstractions.Data;

	using Xunit;

	public class RavenDB_1345 : RavenTest
	{
		[Fact]
		public void ExpandingIndexes()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				var names = new[]
				{
					"Name",
					"SkipAutoSchedule",
					"TagsAsSlugs",
					"LastEditedByUserId",
					"LastEditedAt",
					"ShowPostEvenIfPrivate",
					"IsTrustedCommenter",
					"NumberOfSpamComments",
					"ControllerName",
					"RelatedTwitterNick",
					"RelatedTwitNickDes",
					"PasswordSalt",
					"HashedPassword",
					"AreCommentsClosed",
					"UserHostAddress"
				};

				var str = "";
				foreach (var name in names)
				{
					str += name + ":a ";
					store.DatabaseCommands.Query("dynamic/Posts", new IndexQuery
					{
						Query = str
					}, new string[0]);
				}
			}
		}
	}
}