using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Subscriptions
{
    public class SubscriptionInitialEtagGap : RavenTest
    {
        [Fact]
        public void ShouldSkipInitialUnneccessaryDocuments()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var biPeople = store.BulkInsert())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        biPeople.Store(new Person()
                        {
                            Name = "John Doe #"+i
                        });
                    }
                }
                using (var biPeople = store.BulkInsert())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        biPeople.Store(new Raven.Tests.Common.Dto.Company()
                        {
                            Name = "Something Inc. #" + i
                        }, "companies/"+i);
                    }
                }

                var jsonDocument = store.DatabaseCommands.Get("companies/0");
                var firstCompanyEtag = jsonDocument.Etag;

                WaitForIndexing(store);

                var id = store.Subscriptions.Create(new SubscriptionCriteria<Company>());

                var subscriptionConfig= store.Subscriptions.GetSubscriptions(0, 1024).First();

                Assert.Equal(subscriptionConfig.AckEtag, firstCompanyEtag.DecrementBy(1));

                using (var subscriptionManager = store.Subscriptions.Open<Company>(id, new SubscriptionConnectionOptions()))
                {
                    var cde = new CountdownEvent(50);
                    subscriptionManager.Subscribe(x =>
                    {
                        cde.Signal();
                    });
                    
                    Assert.True(cde.Wait(60*1000));
                }

            }
        }

        [Fact]
        public void ShouldPreventSkippingIfLastModifiedIsMetadataFieldIsGreatedThenIndexTimestemp()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var biPeople = store.BulkInsert())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        biPeople.Store(new Person()
                        {
                            Name = "John Doe #" + i
                        });
                    }
                }
                
                using (var biCompanies = store.BulkInsert())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        biCompanies.Store(new Raven.Tests.Common.Dto.Company()
                        {
                            Name = "Something Inc. #" + i
                        }, "companies/"+i);
                    }
                }

                WaitForIndexing(store);
                var db = servers.First().Server.Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase).Result;
                db.StopBackgroundWorkers();
           

                using (var biCompanies = store.BulkInsert(options:new BulkInsertOptions()
                {
                    OverwriteExisting = true
                }))
                {
                    for (int i = 0; i < 50; i++)
                    {
                        biCompanies.Store(new Raven.Tests.Common.Dto.Company()
                        {
                            Name = "Something Inc. #" + i
                        }, "companies/" + i);
                    }
                }


                var jsonDocument = store.DatabaseCommands.Get("companies/0");
                var firstCompanyEtag = jsonDocument.Etag;
                
                var id = store.Subscriptions.Create(new SubscriptionCriteria<Company>());

                var subscriptionConfig = store.Subscriptions.GetSubscriptions(0, 1024).First();

                Assert.Equal(subscriptionConfig.AckEtag, Etag.Empty);
            }
        }

        [Fact]
        public void ShouldSkipToLatestEtagEvenIfHasNoDocumentsOfDesiredType()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var biPeople = store.BulkInsert())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        biPeople.Store(new Person()
                        {
                            Name = "John Doe #" + i
                        },"people/"+i);
                    }
                }

                WaitForIndexing(store);

                var jsonDocument = store.DatabaseCommands.Get("people/49");

                var lastPersonEtag = jsonDocument.Etag;

                var id = store.Subscriptions.Create(new SubscriptionCriteria<Company>());

                var subscriptionConfig = store.Subscriptions.GetSubscriptions(0, 1024).First();
                
                Assert.Equal(subscriptionConfig.AckEtag, lastPersonEtag);
            }
        }
    }
}
