// -----------------------------------------------------------------------
//  <copyright file="RecoverPendingTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Transactions;
// using Microsoft.VisualBasic.Logging;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Storage;

namespace Raven.Database.Plugins.Builtins
{
    public class RecoverPendingTransactions : IStartupTask
    {
        private ILog logger = LogManager.GetCurrentClassLogger();
        public void Execute(DocumentDatabase database)
        {

            var resourceManagerIds = new HashSet<Guid>();
            var recovery = new List<Action>();
            database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var item in accessor.Lists.Read("Raven/Transactions/Pending", Etag.Empty, null, int.MaxValue))
                {
                    var transactionId = item.Key;
                    try
                    {
                        var ravenJToken = item.Data["Changes"];
                        var changes = ravenJToken.JsonDeserialization<List<DocumentInTransactionData>>();
                        var resourceManagerId = item.Data.Value<string>("ResourceManagerId");
                        var recoveryInformation = item.Data.Value<byte[]>("RecoveryInformation");

                        if(database.InFlightTransactionalState.RecoverTransaction(transactionId, changes) == false)
							continue;
                   
                        Guid resourceId;
                        if (Guid.TryParse(resourceManagerId, out resourceId) == false || 
                            recoveryInformation == null)
                        {
	                        try
	                        {
		                        database.InFlightTransactionalState.Prepare(transactionId, null, null);
	                        }
	                        catch (Exception e)
	                        {
		                        logger.WarnException(
			                        "Failed to prepare transaction " + transactionId +
			                        " on database restart, transaction was aborted", e);
	                        }
                            continue;
                        }
						try
						{
							database.InFlightTransactionalState.Prepare(transactionId, resourceId, recoveryInformation);
						}
						catch (Exception e)
						{
							logger.WarnException(
								"Failed to prepare transaction " + transactionId +
								" on database restart, transaction was aborted", e);
							continue;
						}
                        resourceManagerIds.Add(resourceId);
                        recovery.Add(() =>
                        {
                            try
                            {
                                var enlistmentNotification = new RecoveryEnlistment(database, transactionId);
                                TransactionManager.Reenlist(resourceId, recoveryInformation, enlistmentNotification);
                            }
                            catch (Exception e)
                            {
                                logger.WarnException("Could not apply recovered transaction " + transactionId + ", aborting transaction", e);
                            }
                        });
                    }
                    catch (Exception e)
                    {
                        logger.WarnException("Could not apply recovered transaction " + transactionId +", aborting transaction", e);
                    }
                }

                accessor.Lists.RemoveAllBefore("Raven/Transactions/Pending", Etag.InvalidEtag);
            });

            foreach (var recoverTx in recovery)
            {
                recoverTx();
            }

            foreach (var resourceManagerId in resourceManagerIds)
            {
                try
                {
                    TransactionManager.RecoveryComplete(resourceManagerId);
                }
                catch (Exception e)
                {
                    logger.WarnException(
                        "Could not notify transaction manager that the recovery was completed for " + resourceManagerId,
                        e);
                }
            }
        }
    }
}