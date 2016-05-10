//-----------------------------------------------------------------------
// <copyright file="DeleteRemovedIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
    public class DeleteRemovedIndexes : IStartupTask
    {
        #region IStartupTask Members

        private static ILog log = LogManager.GetCurrentClassLogger();

        public void Execute(DocumentDatabase database)
        {
            var pendingDeletions = new List<RavenJObject>();
            var idsOfLostIndexes = new List<int>();

            database.TransactionalStorage.Batch(actions =>
            {
                foreach (var result in actions.Lists.Read("Raven/Indexes/PendingDeletion", Etag.Empty, null, 100))
                {
                    pendingDeletions.Add(result.Data);
                }
                       
                List<int> indexIds = actions.Indexing.GetIndexesStats().Select(x => x.Id).ToList();
                foreach (int id in indexIds)
                {
                    var index = database.IndexDefinitionStorage.GetIndexDefinition(id);
                    if (index != null)
                        continue;

                    idsOfLostIndexes.Add(id);
                }
            });

            foreach (var pendingDeletion in pendingDeletions)
            {
                database.Indexes.StartDeletingIndexDataAsync(pendingDeletion.Value<int>("IndexId"), pendingDeletion.Value<string>("IndexName"));
            }

            Task.Factory.StartNew(() =>
            {
                foreach (var indexId in idsOfLostIndexes)
                {
                    try
                    {
                        // index is not found on disk, better kill for good
                        // Even though technically we are running into a situation that is considered to be corrupt data
                        // we can safely recover from it by removing the other parts of the index.
                        
                        database.TransactionalStorage.Batch(actions =>
                        {
                            database.IndexStorage.DeleteIndex(indexId);
                            actions.Indexing.DeleteIndex(indexId, database.WorkContext.CancellationToken);
                        });
                    }
                    catch (Exception e)
                    {
                        log.ErrorException("Could not delete data from the storage of an index which was not found on the disk. Index id: " + indexId, e);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        #endregion
    }
}
