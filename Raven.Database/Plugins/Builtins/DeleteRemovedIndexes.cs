//-----------------------------------------------------------------------
// <copyright file="DeleteRemovedIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Plugins.Builtins
{
	public class DeleteRemovedIndexes : IStartupTask
	{
		#region IStartupTask Members

		public void Execute(DocumentDatabase database)
		{
			database.TransactionalStorage.Batch(actions =>
			{
			    foreach (var result in actions.Lists.Read("Raven/Indexes/PendingDeletion", Etag.Empty, null, 100))
			    {
			        database.StartDeletingIndexData(result.Data.Value<int>("IndexId"));
			    }
			           
                List<int> indexIds = actions.Indexing.GetIndexesStats().Select(x => x.Id).ToList();
                foreach (int id in indexIds)
				{
                    var index = database.IndexDefinitionStorage.GetIndexDefinition(id);
                    if (index != null)
						continue;

					// index is not found on disk, better kill for good
					// Even though technically we are running into a situation that is considered to be corrupt data
					// we can safely recover from it by removing the other parts of the index.
                    database.IndexStorage.DeleteIndex(id);
                    actions.Indexing.DeleteIndex(id, database.WorkContext.CancellationToken);
				}
			});
		}

		#endregion
	}
}
