// -----------------------------------------------------------------------
//  <copyright file="DocDbExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Extensions
{
    public static class DocDbExtensions
    {
        private static readonly ILog Logger = LogManager.GetCurrentClassLogger();
        private const int MaxTries = 10;

        public static void AddAlert(this DocumentDatabase self, Alert alert)
        {
            var tries = 0;
            while (true)
            {
                using(self.TransactionalStorage.DisableBatchNesting())
                using (var putSerialLock = self.DocumentLock.TryLock(25))
                {
                    if (putSerialLock == null)
                        continue;

                    AlertsDocument alertsDocument;
                    var alertsDoc = self.Documents.Get(Constants.RavenAlerts, null);
                    RavenJObject metadata;
                    Etag etag;  
                    if (alertsDoc == null)
                    {
                        etag = Etag.Empty;
                        alertsDocument = new AlertsDocument();
                        metadata = new RavenJObject();
                    }
                    else
                    {
                        etag = alertsDoc.Etag;
                        alertsDocument = alertsDoc.DataAsJson.JsonDeserialization<AlertsDocument>() ?? new AlertsDocument();
                        metadata = alertsDoc.Metadata;
                    }

                    var withSameUnique = alertsDocument.Alerts.FirstOrDefault(alert1 => alert1.UniqueKey == alert.UniqueKey);
                    if (withSameUnique != null)
                    {
                        // copy information about observed
                        alert.LastDismissedAt = withSameUnique.LastDismissedAt;
                        alertsDocument.Alerts.Remove(withSameUnique);
                    }

                    alertsDocument.Alerts.Add(alert);
                    var document = RavenJObject.FromObject(alertsDocument);
                    document.Remove("Id");
                    try
                    {
                        self.Documents.Put(Constants.RavenAlerts, etag, document, metadata, null);
                        return;
                    }
                    catch (ConcurrencyException)
                    {
                        //try again...
                    }
                    catch (Exception e)
                    {
                        if (TransactionalStorageHelper.IsOutOfMemoryException(e))
                        {
                            if (tries++ < MaxTries)
                            {
                                Thread.Sleep(11);
                                continue;
                            }

                            Logger.WarnException("Couldn't save alerts document due to " +
                                                 $"{self.TransactionalStorage.FriendlyName} out of memory exception", e);
                            return;
                        }

                        throw;
                    }
                }
            }
        }
    }
}
