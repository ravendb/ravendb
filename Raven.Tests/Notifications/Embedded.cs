// -----------------------------------------------------------------------
//  <copyright file="Embedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Diagnostics;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;
using System.Reactive.Linq;
using System;

namespace Raven.Tests.Notifications
{
    public class Embedded : RavenTest
    {
        public class Item
        {
        }

        [Fact]
        public void CanGetNotificationAboutDocumentPut()
        {
            using (var store = NewDocumentStore())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                store.Changes().Task.Result
                    .ForDocument("items/1").Task.Result
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();
                }

                DocumentChangeNotification documentChangeNotification;
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

                Assert.Equal("items/1", documentChangeNotification.Id);
                Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
            }
        }

        [Fact]
        public void CanGetNotificationAboutDocumentDelete()
        {
            using (var store = NewDocumentStore())
            {
                var list = new BlockingCollection<DocumentChangeNotification>();
                var databaseChanges = store.Changes();
                databaseChanges.Task.Wait();
                var documentSubscription = store.Changes().ForDocument("items/1");
                documentSubscription.Task.Wait();
                documentSubscription
                    .Where(x => x.Type == DocumentChangeTypes.Delete)
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();
                }

                store.DatabaseCommands.Delete("items/1", null);

                DocumentChangeNotification documentChangeNotification;
                Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(5)));

                Assert.Equal("items/1", documentChangeNotification.Id);
                Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Delete);
            }
        }

        [Fact]
        public void CanGetNotificationAboutDocumentIndexUpdate()
        {
            using (var store = NewDocumentStore())
            {
                var list = new BlockingCollection<IndexChangeNotification>();
                var databaseChanges = store.Changes();
                databaseChanges.Task.Wait();
                var indexSubscription = databaseChanges.ForIndex("Raven/DocumentsByEntityName");
                indexSubscription.Task.Wait();
                indexSubscription
                    .Where(x => x.Type == IndexChangeTypes.MapCompleted)
                    .Subscribe(list.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();
                }

                IndexChangeNotification changeNotification;
                Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(3)));

                Assert.Equal("Raven/DocumentsByEntityName", changeNotification.Name);
                Assert.Equal(changeNotification.Type, IndexChangeTypes.MapCompleted);
            }
        }
    }
}
