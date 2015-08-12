// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3760.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Replication;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3760 : ReplicationBase
	{
		[Fact]
		public void destination_with_specified_collections_is_not_considered_as_failover_target()
		{
			var destination = new ReplicationDestination
			{
				SpecifiedCollections = new Dictionary<string, string>()
				{
					{
						"Orders", null
					},
				}
			};

			Assert.False(destination.IgnoredClient);
		}

		[Fact]
		public void can_use_metadata_in_transform_script()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				RunReplication(master, slave, specifiedCollections: new Dictionary<string, string>
				{
					{
						"users", @"this.Name =  this['@metadata']['User']"
					}
				});

				using (var session = master.OpenSession())
				{
					var entity = new User
					{
						Name = "foo"
					};

					session.Store(entity, "users/1");
					session.Advanced.GetMetadataFor(entity)["User"] = "bar";

					session.SaveChanges();
				}

				WaitForReplication(slave, "users/1");

				using (var session = slave.OpenSession())
				{
					var user = session.Load<User>("users/1");

					Assert.Equal("bar", user.Name);
				}
			}
		}

		[Fact]
		public void null_returned_from_script_means_that_document_is_filtered_out()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				RunReplication(master, slave, specifiedCollections: new Dictionary<string, string>
				{
					{
						"users", @"if (this.Age % 2 == 0) return null; else this.Name = 'transformed'; return this;"
					}
				});

				const int count = 10;

				using (var session = master.OpenSession())
				{
					for (int i = 0; i < count; i++)
					{
						session.Store(new User
						{
							Age = i
						}, "users/" + i);
					} 
					
					session.SaveChanges();
				}

				WaitForReplication(slave, "users/" + (count - 1));

				using (var session = slave.OpenSession())
				{
					for (int i = 0; i < count; i++)
					{
						var user = session.Load<User>("users/" + i);

						if (i%2 == 0)
						{
							Assert.Null(user);
						}
						else
						{
							Assert.Equal("transformed", user.Name);
						}
					}
				}
			}
		}

		[Fact]
		public void null_script_means_no_transformation_nor_filtering_withing_specified_collection()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				RunReplication(master, slave, specifiedCollections: new Dictionary<string, string>
				{
					{
						"users", null
					}
				});

				using (var session = master.OpenSession())
				{
					session.Store(new Person
					{
						Name = "Arek"
					}, "people/1");

					session.Store(new User
					{
						Name = "Arek"
					}, "users/1");

					session.SaveChanges();
				}

				WaitForReplication(slave, "users/1");

				using (var session = slave.OpenSession())
				{
					var user = session.Load<User>("users/1");

					Assert.Equal("Arek", user.Name);

					var person = session.Load<Person>("people/1");

					Assert.Null(person);
				}
			}
		}
	}
}