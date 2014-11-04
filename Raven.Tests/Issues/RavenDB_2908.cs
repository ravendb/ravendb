// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2908.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Client.Listeners;

using Xunit;

namespace Raven.Tests.Issues
{
	using Raven.Tests.Bundles.Replication;

	public class RavenDB_2908 : ReplicationBase
	{
		private class Person
		{
			public string Id { get; set; }

			public string Name { get; set; }
		}

		private class CustomConflictListener : IDocumentConflictListener
		{
			public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
			{
				conflictedDocs[0].Metadata.Remove("@id");
				conflictedDocs[0].Metadata.Remove("@etag");
				resolvedDocument = conflictedDocs[0];
				return true;
			}
		}

		private class SimpleTransformer : AbstractTransformerCreationTask<Person>
		{
			public class Result
			{
				public string Name { get; set; }
			}

			public SimpleTransformer()
			{
				TransformResults = people => from person in people select new { Name = person.Name + "/Transformed" };
			}
		}

		[Fact]
		public void LazyLoadShouldResolveConfictsProperly1()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new Person { Name = "p1" });
					s1.Store(new Person { Name = "p2" });
					s1.SaveChanges();
				}

				using (var s2 = store2.OpenSession())
				{
					s2.Store(new Person { Name = "p3" });
					s2.Store(new Person { Name = "p4" });
					s2.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(store2,
					session =>
					{
						try
						{
							session.Load<Person>("people/1");
							return false;
						}
						catch (Exception)
						{
							return true;
						}
					});

				using (var s2 = store2.OpenSession())
				{
					Assert.Throws<ConflictException>(() => s2.Load<Person>("people/1"));
				}

				((DocumentStore)store1).RegisterListener(new CustomConflictListener());
				((DocumentStore)store2).RegisterListener(new CustomConflictListener());

				using (var s2 = store2.OpenSession())
				{
					var person = s2
						.Advanced
						.Lazily
						.Load<Person>("people/1").Value;

					Assert.Equal("p3", person.Name);
				}
			}
		}

		[Fact]
		public void LazyLoadShouldResolveConfictsProperly2()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new Person { Name = "p1" });
					s1.Store(new Person { Name = "p2" });
					s1.SaveChanges();
				}

				using (var s2 = store2.OpenSession())
				{
					s2.Store(new Person { Name = "p3" });
					s2.Store(new Person { Name = "p4" });
					s2.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(store2,
					session =>
					{
						try
						{
							session.Load<Person>("people/1");
							return false;
						}
						catch (Exception)
						{
							return true;
						}
					});

				using (var s2 = store2.OpenSession())
				{
					Assert.Throws<ConflictException>(() => s2.Advanced.Lazily.Load<Person>(new[] { "people/1", "people/2" }).Value);
				}

				((DocumentStore)store1).RegisterListener(new CustomConflictListener());
				((DocumentStore)store2).RegisterListener(new CustomConflictListener());

				using (var s2 = store2.OpenSession())
				{
					var people = s2
						.Advanced
						.Lazily
						.Load<Person>(new[] { "people/1", "people/2" }).Value;

					Assert.Equal(2, people.Length);
					Assert.True(people.Any(x => x.Name == "p3"));
					Assert.True(people.Any(x => x.Name == "p4"));
				}
			}
		}

		[Fact]
		public void LoadShouldResolveConfictsProperly()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new Person { Name = "p1" });
					s1.Store(new Person { Name = "p2" });
					s1.SaveChanges();
				}

				using (var s2 = store2.OpenSession())
				{
					s2.Store(new Person { Name = "p3" });
					s2.Store(new Person { Name = "p4" });
					s2.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(store2,
					session =>
					{
						try
						{
							session.Load<Person>("people/1");
							return false;
						}
						catch (Exception)
						{
							return true;
						}
					});

				using (var s2 = store2.OpenSession())
				{
					Assert.Throws<ConflictException>(() => s2.Load<Person>(new[] { "people/1", "people/2" }));
				}

				((DocumentStore)store1).RegisterListener(new CustomConflictListener());
				((DocumentStore)store2).RegisterListener(new CustomConflictListener());

				using (var s2 = store2.OpenSession())
				{
					var people = s2
						.Load<Person>(new[] { "people/1", "people/2" });

					Assert.Equal(2, people.Length);
					Assert.True(people.Any(x => x.Name == "p3"));
					Assert.True(people.Any(x => x.Name == "p4"));
				}
			}
		}

		[Fact]
		public void LoadWithTransformerShouldThrowIfTransformedDocumentsAreInConflict()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				new SimpleTransformer().Execute(store1);
				new SimpleTransformer().Execute(store2);

				using (var s1 = store1.OpenSession())
				{
					s1.Store(new Person { Name = "p1" });
					s1.Store(new Person { Name = "p2" });
					s1.SaveChanges();
				}

				using (var s2 = store2.OpenSession())
				{
					s2.Store(new Person { Name = "p3" });
					s2.Store(new Person { Name = "p4" });
					s2.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(store2,
					session =>
					{
						try
						{
							session.Load<Person>("people/1");
							return false;
						}
						catch (Exception)
						{
							return true;
						}
					});

				using (var s2 = store2.OpenSession())
				{
					Assert.Throws<ConflictException>(
						() => s2.Load<SimpleTransformer, SimpleTransformer.Result>(new[] { "people/1", "people/2" }));
				}

				using (var s2 = store2.OpenSession())
				{
					Assert.Throws<ConflictException>(
						() => s2.Load<SimpleTransformer, SimpleTransformer.Result>("people/1"));
				}

				((DocumentStore)store1).RegisterListener(new CustomConflictListener());
				((DocumentStore)store2).RegisterListener(new CustomConflictListener());

				using (var s2 = store2.OpenSession())
				{
					var person1 = s2.Load<SimpleTransformer, SimpleTransformer.Result>("people/1");
					Assert.Equal("p3/Transformed", person1.Name);
				}

				using (var s2 = store2.OpenSession())
				{
					var people = s2.Load<SimpleTransformer, SimpleTransformer.Result>(new[] { "people/1", "people/2" });

					Assert.Equal(2, people.Length);
					Assert.True(people.Any(x => x.Name == "p3/Transformed"));
					Assert.True(people.Any(x => x.Name == "p4/Transformed"));
				}
			}
		}
	}
}