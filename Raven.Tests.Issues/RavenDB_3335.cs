// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3335.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3335 : ReplicationBase
	{
		private class UserJoiningConflictListener : IDocumentConflictListener
		{
			public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
			{
				resolvedDocument = new JsonDocument
				{
					DataAsJson = new RavenJObject
					{
						{"Name", string.Join(" ", conflictedDocs.Select(x => x.DataAsJson.Value<string>("Name")).OrderBy(x => x))}
					},
					Metadata = new RavenJObject
					{
						{ Constants.RavenEntityName, "Users" }
					}
				};
				return true;
			}
		}

		private class TakeNewerUserConflictListener : IDocumentConflictListener
		{
			public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
			{
				var newer = conflictedDocs.Max(x => x.LastModified);

				resolvedDocument = conflictedDocs.First(x => x.LastModified == newer);

				resolvedDocument.Metadata.Remove("@id");
				resolvedDocument.Metadata.Remove("@etag");

				resolvedDocument.Metadata.Remove(Constants.RavenReplicationConflict);
				resolvedDocument.Metadata.Remove(Constants.RavenReplicationConflictDocument);

				return true;
			}
		}

		private class SimpleUserIndex : AbstractIndexCreationTask<User>
		{
			public SimpleUserIndex()
			{
				Map = users => from u in users
					select new
					{
						u.Name
					};
			}
		}

		[Fact]
		public void QueryShouldResolveConflictByUsingRegisteredListener()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				new SimpleUserIndex().Execute(slave);

				using (var s = master.OpenSession())
				{
					s.Store(new User
					{
						Name = "James"
					});
					s.SaveChanges();
				}

				using (var s = slave.OpenSession())
				{
					s.Store(new User
					{
						Name = "Smith"
					});
					s.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(slave, session =>
					{
						try
						{
							session.Load<User>("users/1");
							return false;
						}
						catch (Exception)
						{
							return true;
						}
					});

				using (var s = slave.OpenSession())
				{
					Assert.Throws<ConflictException>(() => s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).ToList());
				}

				slave.RegisterListener(new UserJoiningConflictListener());

				using (var s = slave.OpenSession())
				{
					var users = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();

					Assert.Equal(1, users.Count);
					Assert.Equal("James Smith", users[0].Name);
				}
			}
		}

		[Fact]
		public void IndexShouldBeAwareOfValuesInConflictedDocs()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				new SimpleUserIndex().Execute(slave);

				using (var s = master.OpenSession())
				{
					s.Store(new User
					{
						Name = "James"
					});
					s.SaveChanges();
				}

				using (var s = slave.OpenSession())
				{
					s.Store(new User
					{
						Name = "David"
					});
					s.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(slave, session =>
				{
					try
					{
						session.Load<User>("users/1");
						return false;
					}
					catch (Exception)
					{
						return true;
					}
				});

				using (var s = slave.OpenSession())
				{
					Assert.Throws<ConflictException>(() => s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).ToList());
				}

				slave.RegisterListener(new TakeNewerUserConflictListener());

				using (var s = slave.OpenSession())
				{
					// we know that we replicated document with Name == "David" so we should be able to query by it and under the hood the conflict should be resolved
					var users = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "David").ToList();

					Assert.Equal(1, users.Count);
					Assert.Equal("David", users[0].Name);
				}
			}
		}

		[Fact]
		public void QueryShouldResolveConflictByUsingRegisteredListener_Paging()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				new SimpleUserIndex().Execute(slave);

				using (var s = master.OpenSession())
				{
					s.Store(new User
					{
						Name = "James"
					});

					s.Store(new User
					{
						Name = "David"
					});

					s.SaveChanges();
				}

				using (var s = slave.OpenSession())
				{
					s.Store(new User
					{
						Name = "Smith"
					});

					s.Store(new User
					{
						Name = "Johnson"
					});

					s.SaveChanges();
				}

				master.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(slave, "marker");

				using (var s = slave.OpenSession())
				{
					Assert.Throws<ConflictException>(() => s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).ToList());
				}

				slave.RegisterListener(new UserJoiningConflictListener());

				using (var s = slave.OpenSession())
				{
					var users = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
					Assert.Equal(2, users.Count);
					Assert.Equal("James Smith", users[0].Name);
					Assert.Equal("David Johnson", users[1].Name);

					users = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).Take(1).ToList();
					Assert.Equal(1, users.Count);
					Assert.Equal("James Smith", users[0].Name);

					users = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).Skip(1).Take(1).ToList();
					Assert.Equal(1, users.Count);
					Assert.Equal("David Johnson", users[0].Name);

					users = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).Skip(3).Take(1).ToList();
					Assert.Equal(0, users.Count);

					users = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).Skip(2).Take(1).ToList();
					Assert.Equal(0, users.Count);
				}
			}
		}

		[Fact]
		public void LazyQueryShouldResolveConflictByUsingRegisteredListener()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				new SimpleUserIndex().Execute(slave);

				using (var s = master.OpenSession())
				{
					s.Store(new User
					{
						Name = "James"
					});
					s.SaveChanges();
				}

				using (var s = slave.OpenSession())
				{
					s.Store(new User
					{
						Name = "Smith"
					});
					s.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(slave, session =>
				{
					try
					{
						session.Load<User>("users/1");
						return false;
					}
					catch (Exception)
					{
						return true;
					}
				});

				using (var s = slave.OpenSession())
				{
					Assert.Throws<ConflictException>(() => s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).Lazily().Value.ToList());
				}

				slave.RegisterListener(new UserJoiningConflictListener());

				using (var s = slave.OpenSession())
				{
					var lazyQuery = s.Query<User, SimpleUserIndex>().Customize(x => x.WaitForNonStaleResults()).Lazily();

					var users = lazyQuery.Value.ToList();

					Assert.Equal(1, users.Count);
					Assert.Equal("James Smith", users[0].Name);
				}
			}
		}
	}
}