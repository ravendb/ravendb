// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3639.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3639 : ReplicationBase
	{
		protected override void SetupDestination(ReplicationDestination replicationDestination)
		{
			replicationDestination.SpecifiedCollections = new Dictionary<string, string>
			{
				{
					"users", @"this.Name = 'patched ' + this.Name;"
				}
			};
		}

		[Fact]
		public void replicated_docs_are_transformed_according_to_provided_collection_specific_scripts()
		{
			using (var master = CreateStore())
			using (var slave = CreateStore())
			{
				RunReplication(master, slave);

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

					Assert.Equal("patched Arek", user.Name);

					var person = session.Load<Person>("people/1");

					Assert.Null(person);
				}
			}
		}
	}
}