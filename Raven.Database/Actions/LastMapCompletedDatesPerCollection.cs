// -----------------------------------------------------------------------
//  <copyright file="LastIndexDatesPerCollection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;

namespace Raven.Database.Actions
{
    public class LastMapCompletedDatesPerCollection
    {
        private readonly ConcurrentDictionary<string, DateTime> lastIndexingTime = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        public LastMapCompletedDatesPerCollection(DocumentDatabase documentDatabase)
        {
            documentDatabase.Notifications.OnIndexChange += (database, notification) =>
            {
                if (notification.Type == IndexChangeTypes.MapCompleted && notification.Name == Constants.DocumentsByEntityNameIndex)
                {
                    Update(notification.Collections);
                }
            };
        }

        public void Update(HashSet<string> collections)
        {
            var now = SystemTime.UtcNow;
            foreach (var collection in collections)
            {
                lastIndexingTime[collection] = now;
            }
        }

        public List<string> GetLastChangedCollections(DateTime date)
        {
            return lastIndexingTime
                .Where(x => x.Value > date)
                .Select(x => x.Key)
                .ToList();
        }
    }
}