// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3373.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3373 : RavenTest
	{
		public class Users_ByName : AbstractIndexCreationTask<User>
		{
			public Users_ByName()
			{
				Map = users => from u in users select new { u.Name};
			}
		}
		[Fact]
		public void CanSearchAfterForcedCommitDataFlushes()
		{
			var dataDir = NewDataPath();
			using (var store = NewDocumentStore(runInMemory: false, dataDir: dataDir))
			{
				new Users_ByName().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						Name = "One",
					});

					session.Store(new User
					{
						Name = "Two",
					});

					session.Store(new User
					{
						Name = "Three",
					});

					session.SaveChanges();

					Assert.Equal(3, session.Query<User, Users_ByName>().Customize(x => x.WaitForNonStaleResults()).ToList().Count);

					store.DocumentDatabase.IndexStorage.FlushMapIndexes();

					SystemTime.UtcDateTime = () => DateTime.Now.AddHours(1);
					session.Store(new Address());
					session.SaveChanges();
					WaitForIndexing(store);

					store.DocumentDatabase.IndexStorage.FlushMapIndexes();

					SystemTime.UtcDateTime = () => DateTime.Now.AddHours(2);
					session.Store(new Address());
					session.SaveChanges();
					WaitForIndexing(store);

					store.DocumentDatabase.IndexStorage.FlushMapIndexes();

					SystemTime.UtcDateTime = () => DateTime.Now.AddHours(3);
					session.Store(new Address());
					session.SaveChanges();
					WaitForIndexing(store);

					store.DocumentDatabase.IndexStorage.FlushMapIndexes();

				}
			}

			using (var store = NewDocumentStore(runInMemory: false, dataDir: dataDir))
			{
				using (var session = store.OpenSession())
				{
					var result = session.Query<User, Users_ByName>().Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(3, result.Count);

					session.Store(new User
					{
						Name = "Four",
					});

					session.SaveChanges();

					result = session.Query<User, Users_ByName>().Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(4, result.Count);
				}
			}
		}
	}
}