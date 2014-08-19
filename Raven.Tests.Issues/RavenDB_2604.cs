// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2604.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2604 : ReplicationBase
	{
		[Fact]
		public void BulkInsertShouldNotUpdateDocsIfTheyHaveTheSameContent()
		{
			using (var store = CreateStore())
			{
				var keyEtagCollection = new Dictionary<string, Etag>();
				var entities = new List<User>();

				using (var session = store.OpenSession())
				{
					for (var i = 0; i < 10; i++)
					{
						var entity = new User()
						{
							Id = "users/" + i,
							Name = "Name/" + i
						};
						session.Store(entity);

						entities.Add(entity);
					}

					session.SaveChanges();

					foreach (var user in entities)
					{
						keyEtagCollection.Add(session.Advanced.GetDocumentId(user), session.Advanced.GetEtagFor(user));
					}
				}

				using (var bulk = store.BulkInsert(options: new BulkInsertOptions
				{
					OverwriteExisting = true,
					SkipOverwriteIfUnchanged = true
				}))
				{
					for (var i = 0; i < 10; i++)
					{
						var entity = new User()
						{
							Id = "users/" + i,
							Name = "Name/" + i + ((i % 2 == 0) ? " Changed" : string.Empty)
						};

						bulk.Store(entity);
					}
				}

				for (var i = 0; i < 10; i++)
				{
					var jsonDocument = store.DatabaseCommands.Get("users/" + i);

					if (i % 2 == 0)
						Assert.NotEqual(keyEtagCollection["users/" + i], jsonDocument.Etag);
					else
						Assert.Equal(keyEtagCollection["users/" + i], jsonDocument.Etag);
				}
			}
		}
	}
}