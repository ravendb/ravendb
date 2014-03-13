// -----------------------------------------------------------------------
//  <copyright file="NotificationActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class NotificationActions
    {
        public void RaiseNotifications(DocumentChangeNotification obj, RavenJObject metadata)
        {
            TransportState.Send(obj);
            var onDocumentChange = OnDocumentChange;
            if (onDocumentChange != null)
                onDocumentChange(this, obj, metadata);
        }

        public void RaiseNotifications(IndexChangeNotification obj)
        {
            TransportState.Send(obj);
        }

        public void RaiseNotifications(ReplicationConflictNotification obj)
        {
            TransportState.Send(obj);
        }

        public void RaiseNotifications(BulkInsertChangeNotification obj)
        {
            TransportState.Send(obj);
        } 
    }
}