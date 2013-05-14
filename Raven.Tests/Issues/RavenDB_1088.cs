// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1088.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
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
				store.Changes()
					.ForAllDocuments()
					 .Subscribe(change =>
					 {
						 if (change.Type == DocumentChangeTypes.BulkInsertError)
						 {
							 errored = true;
						 }
					 });

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
						for (int i = 0; i < 1000; i++)
						{
							bulk.Store(new Person
							{
								Id = i,
								FirstName = "FName" + i
							}, i.ToString());
						}
					}
				}
				catch (AggregateException e)
				{
					Assert.Equal("Cannot insert document 0 because it already exists", e.GetBaseException().Message);
				}
			}

			Assert.True(errored);
		}
	}
}