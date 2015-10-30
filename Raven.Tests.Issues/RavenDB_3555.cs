// -----------------------------------------------------------------------
//  <copyright file="RavenDB_abc.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3555 : RavenTest
    {
        [Fact]
        public void ShouldRenewConnectionAndProperlyGetNotification()
        {
            using (var store = NewDocumentStore())
            {
                var firstAllDocsSubscription = store.Changes().Task.Result
                    .ForAllDocuments()
                    .Subscribe(x =>
                    {
                        
                    });

                store.Changes().WaitForAllPendingSubscriptions();

                var allDocsObservable = store.Changes().Task.Result
                    .ForAllDocuments();

                firstAllDocsSubscription.Dispose();

                var items = new BlockingCollection<string>();

                allDocsObservable.Subscribe(x => items.Add(x.Id));

                allDocsObservable.Task.Wait();

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.SaveChanges();
                }

                string userId;
                Assert.True(items.TryTake(out userId, TimeSpan.FromSeconds(10)));
                Assert.Equal("users/1", userId);
            }
        }
    }
}
