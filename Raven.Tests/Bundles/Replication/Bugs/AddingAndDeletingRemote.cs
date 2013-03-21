// -----------------------------------------------------------------------
//  <copyright file="AddingAndDeletingRemote.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Lucene.Net.Util;
using Raven.Json.Linq;
using Xunit;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class AddingAndDeletingRemote : ReplicationBase
	{
		[Fact]
		public void Should_not_cause_issues()
		{
			using (var store1 = CreateStore())
			{
				for (int i = 0; i < 1000; i++)
				{
					store1.DatabaseCommands.Put("test", null, new RavenJObject(), new RavenJObject());
					store1.DatabaseCommands.Delete("test", null);
				}

				store1.DatabaseCommands.Put("test", null, new RavenJObject(), new RavenJObject());
				var replHistory = store1.DatabaseCommands.Get("test").Metadata.Value<RavenJArray>(Constants.RavenReplicationHistory);
				Assert.Equal(50, replHistory.Length);
			}
		}

		[Fact]
		public void Adding_a_lot()
		{
			using (var store1 = CreateStore())
			{
				for (int i = 0; i < 1000; i++)
				{
					store1.DatabaseCommands.Put("test", null, new RavenJObject(), new RavenJObject());
				}

				var replHistory = store1.DatabaseCommands.Get("test").Metadata.Value<RavenJArray>(Constants.RavenReplicationHistory);
				Assert.Equal(50, replHistory.Length);
			}
		}
	}
}