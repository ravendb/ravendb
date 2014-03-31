// -----------------------------------------------------------------------
//  <copyright file="BulkInsertWithChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;
using System;

namespace Raven.Tests.MailingList
{
	public class BulkInsertWithChanges : RavenTest
	{
		[Fact]
		public void StartsWithChangesThrowsWithBulkInsert()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				Exception e = null;
				store.Changes().ForDocumentsStartingWith("something").Subscribe(notification => { }, exception => e = exception);

				using (var session = store.BulkInsert())
				{
					session.Store(new Company(), "else/1");
				}

				Assert.Null(e);
			}
		} 
	}
}