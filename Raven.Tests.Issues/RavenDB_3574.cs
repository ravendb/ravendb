using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Replication.Tasks;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3574 : ReplicationBase
	{
		[Fact]
		public void Index_replication_with_side_by_side_indexes_should_not_propagate_replaced_index_tombstones()
		{
			using(var source = CreateStore())
			using (var destination = CreateStore())
			{
				var oldIndexDef = new IndexDefinition
				{
					Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}"
				};
				var testIndex = new RavenDB_3232.TestIndex();

				servers[0].SystemDatabase.StopBackgroundWorkers();
				source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);
				using (var session = source.OpenSession())
				{
					session.Store(new RavenDB_3232.Person { FirstName = "John", LastName = "Doe" });
					session.SaveChanges();
				}
				var sourceReplicationTask = servers[0].SystemDatabase.StartupTasks.OfType<ReplicationTask>().First();
				sourceReplicationTask.Pause(); //pause replciation task _before_ setting up replication

				TellFirstInstanceToReplicateToSecondInstance();

				testIndex.SideBySideExecute(source);

				servers[0].SystemDatabase.SpinBackgroundWorkers();
				WaitForIndexing(source); //now old index should be a tombstone and side-by-side replaced it.


			}
		}
	}
}
