//-----------------------------------------------------------------------
// <copyright file="AsyncCommit.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class AsyncCommit : RavenTest
	{
		[Fact]
		public void DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalse()
		{
			using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(documentStore);

				using (var s = documentStore.OpenSession())
				{
					s.Store(new AccurateCount.User { Name = "Ayende" });
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				using (var scope = new TransactionScope())
				{
					var user = s.Load<AccurateCount.User>("users/1");
					user.Name = "Rahien";
					s.SaveChanges();
					scope.Complete();
				}

				using (var s = documentStore.OpenSession())
				{
					s.Advanced.AllowNonAuthoritativeInformation = false;
					var user = s.Load<AccurateCount.User>("users/1");
					Assert.Equal("Rahien", user.Name);
				}
			}
		}

		[Fact]
		public void DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalseWhenQuerying()
		{
            using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(documentStore);

				documentStore.DatabaseCommands.PutIndex("test",
														new IndexDefinition
														{
															Map = "from doc in docs select new { doc.Name }"
														});

				using (var s = documentStore.OpenSession())
				{
					s.Store(new AccurateCount.User { Name = "Ayende" });
					s.SaveChanges();

					s.Advanced.DocumentQuery<AccurateCount.User>("test")
						.WaitForNonStaleResults()
						.FirstOrDefault();
				}

				var task = new Task(() =>
				{
					using (var s = documentStore.OpenSession())
					{
						s.Advanced.AllowNonAuthoritativeInformation = false;
						var user = s.Advanced.DocumentQuery<AccurateCount.User>("test")
							.FirstOrDefault();
						Assert.Equal("Rahien", user.Name);
					}
				});
				using (var s = documentStore.OpenSession())
				using (var scope = new TransactionScope())
				{
					var user = s.Load<AccurateCount.User>("users/1");
					user.Name = "Rahien";
					s.SaveChanges();
					task.Start();
					Thread.Sleep(250);
					scope.Complete();
				}
				task.Wait();
			}
		}
	}
}
