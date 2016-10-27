using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Tests.Common.Dto;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5511:RavenTestBase
    {

        public class TimedOutObservable<T> : IObserver<T>
        {
            private readonly CountdownEvent cde;

            public TimedOutObservable(CountdownEvent cde)
            {
                this.cde = cde;
            }
            public void OnNext(T value)
            {
                Thread.Sleep(100);
            }

            public void OnError(Exception error)
            {
                if (error is SubscriptionAckTimeoutException)
                {
                    cde.Signal();
                }
            }

            public void OnCompleted()
            {
                
            }
        }

        [Fact]
        public void SubscriptionAckTimeoutShouldNotifyClients()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var biPeople = store.BulkInsert())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        biPeople.Store(new Person()
                        {
                            Name = "John Doe #" + i
                        }, "people/" + i);
                    }
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria<Person>());

                using (var subscription = store.Subscriptions.Open<Person>(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions()
                    {
                        AcknowledgmentTimeout = TimeSpan.FromMilliseconds(100)
                    }
                }))
                {
                    var mre = new ManualResetEvent(false);
                    CountdownEvent cte = new CountdownEvent(2);
                    var obs = new TimedOutObservable<Person>(cte);
                    subscription.AfterBatch += _ => mre.Set();
                    subscription.Subscribe(obs);
                    Assert.True(cte.Wait(3000));
                    Assert.True(mre.WaitOne(3000));
                }
            }
        }
    }
}
