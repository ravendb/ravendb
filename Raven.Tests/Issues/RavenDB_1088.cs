// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1088.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1088 : RavenTest
	{
		internal class Person
		{
			public int Id { get; set; }

			public string FirstName { get; set; }
		}

		[Fact]
		public void BulkInsertErrorNotificationTest()
		{
			var errored = false;

			using (var store = NewDocumentStore())
			{
				using (var bulk = store.BulkInsert())
				{
					for (int i = 0; i < 1000; i++)
					{
						bulk.Store(new Person
						{
							Id = i,
							FirstName = "FName" + i
						}, i.ToString());
					}
				}

				try
				{
					using (var bulk = store.BulkInsert())
					{
						store.Changes()
							 .ForBulkInsert(bulk.OperationId)
							 .Subscribe(change =>
							 {
								 if (change.Type == DocumentChangeTypes.BulkInsertError)
								 {
									 errored = true;
								 }
							 });

						for (int i = 0; i < 1000; i++)
						{
							bulk.Store(new Person
							{
								Id = i,
								FirstName = "FName" + i
							}, i.ToString());
						}
					}

					Assert.True(false);
				}
				catch (Exception e)
				{
					Assert.True(e.GetBaseException().Message.StartsWith("Cannot insert") || // munin
						e.GetBaseException().Message.StartsWith("Illegal duplicate key") || //esent
						e.GetBaseException().Message.StartsWith("InsertDocument() - checkForUpdates is false")); 
				}
			}

			Assert.True(errored);
		}

		[Fact]
		public void BulkInsertErrorNotificationRemoteTest()
		{
			var errored = false;

			using (var store = NewRemoteDocumentStore())
			{
				using (var bulk = store.BulkInsert())
				{
					for (int i = 0; i < 1000; i++)
					{
						bulk.Store(new Person
						{
							Id = i,
							FirstName = "FName" + i
						}, i.ToString());
					}
				}

				using (var bulk = store.BulkInsert())
				{
					store.Changes()
						 .ForBulkInsert(bulk.OperationId)
						 .Subscribe(change =>
						 {
							 if (change.Type == DocumentChangeTypes.BulkInsertError)
							 {
								 errored = true;
							 }
						 });

					for (int i = 0; i < 1000; i++)
					{
						try
						{
							bulk.Store(new Person
							{
								Id = i,
								FirstName = "FName" + i
							}, i.ToString(CultureInfo.InvariantCulture));
						}
						catch (Exception)
						{
							errored = true;
							break;
						}
					}

					try
					{
						bulk.DisposeAsync().Wait();
					}
					catch (Exception)
					{
						errored = true;
					}
				}
			}

			Assert.True(errored);
		}
	}
}