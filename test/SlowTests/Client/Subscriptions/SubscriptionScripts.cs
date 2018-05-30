using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class SubscriptionScripts : RavenTestBase
    {
        [Fact]
        public async Task CanHandleAny()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall()
                    {
                        Name = "James",
                        SupportCallNumber = 1,
                        Comments = new List<string>
                        {
                            "cool", "great", "nice"
                        }
                    });

                    await session.StoreAsync(new SupportCall()
                    {
                        Name = "James",
                        SupportCallNumber = 2,
                        Comments = new List<string>
                        {
                            "bad", "very annoying"
                        }
                    });

                    await session.StoreAsync(new SupportCall()
                    {
                        Name = "Aviv",
                        SupportCallNumber = 3,
                        Comments = new List<string>
                        {
                            "this is annoying"
                        }
                    });                    

                    await session.SaveChangesAsync();
                }
                

                
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Name == "James" && call.Comments.Any(comment => comment.Contains("annoying"))
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("James", call.Name);
                    Assert.Equal(2, call.SupportCallNumber);                   
                    Assert.False(users.TryTake(out call, 50));


                }
            }
        }

        [Fact]
        public async Task CanHandleAll()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "James",
                        SupportCallNumber = 1,
                        Comments = new List<string>
                        {
                            "this is very nice", "was nice to use this"
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "James",
                        SupportCallNumber = 2,
                        Comments = new List<string>
                        {
                            "not nice at all", "bad"
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        SupportCallNumber = 3,
                        Comments = new List<string>
                        {
                            "very nice"
                        }
                    });

                    await session.SaveChangesAsync();
                }

                
                
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Name == "James" && call.Comments.All(comment => comment.Contains("nice")));

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("James", call.Name);
                    Assert.Equal(1, call.SupportCallNumber);
                    Assert.False(users.TryTake(out call, 50));


                }
            }
        }

        [Fact]
        public async Task CanHandleAll_Nested()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Node
                    {
                        Name = "Node1",
                        Children = new List<Node>
                        {
                            new Node { Name = "Child1_1" , Children = new List<Node>
                            {
                                new Node { Name = "a"},
                                new Node { Name = "b"}

                            }},
                            new Node {Name = "Child1_2" , Children = new List<Node>
                            {
                                new Node { Name = "x"},
                                new Node { Name = "y"}
                            }}                            
                        }
                    });

                    await session.StoreAsync(new Node
                    {
                        Name = "Node2",
                        Children = new List<Node>
                        {
                            new Node { Name = "Child2_1" , Children = new List<Node>
                            {
                                new Node { Name = "Parent"},
                                new Node { Name = "Parent"}
                            }}
                        }
                    });

                    await session.StoreAsync(new Node
                    {
                        Name = "Node3",
                        Children = new List<Node>
                        {
                            new Node { Name = "Child3_1" , Children = new List<Node>
                            {
                                new Node { Name = "Parent"},
                                new Node { Name = "z"}
                            }}
                        }
                    });

                    await session.SaveChangesAsync();
                }

                var id = await store.Subscriptions.CreateAsync<Node>(
                    node => node.Children.All(x =>
                        x.Children.All(i => i.Name == "Parent")));

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<Node>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var nodes = new BlockingCollection<Node>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            nodes.Add(item.Result);
                        }
                    }));

                    Assert.True(nodes.TryTake(out var node, 5000));
                    Assert.Equal("Node2", node.Name);
                    Assert.False(nodes.TryTake(out node, 5));
                }
            }
        }

        [Fact]
        public async Task CanHandleWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "James",
                        SupportCallNumber = 1,
                        Comments = new List<string>
                        {
                            "this is very nice", "was nice and very helpful"
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "James",
                        SupportCallNumber = 2,
                        Comments = new List<string>
                        {
                            "not nice at all", "bad"
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        SupportCallNumber = 3,
                        Comments = new List<string>
                        {
                            "very cool", "nice"
                        }
                    });

                    await session.SaveChangesAsync();
                }
                

                
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Comments.Where(comment => comment.Contains("nice")).All(comment => comment.Contains("very"))
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("James", call.Name);
                    Assert.Equal(1, call.SupportCallNumber);
                    Assert.False(users.TryTake(out call, 50));


                }
            }
        }

        [Fact]
        public async Task CanHandleSelect()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "James",
                        SupportCallNumber = 1,
                        Comments = new List<string>
                        {
                            "this was very annoying", "i don't think i will use this again"
                        },
                        Contacts = new List<Person>
                        {
                            new Person
                            {
                                Name = "Michael",
                                Phone = 123456,
                            },
                            new Person
                            {
                                Name = "Maxim",
                                Phone = 9876543,
                            }
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "James",
                        SupportCallNumber = 2,
                        Comments = new List<string>
                        {
                            "hi"
                        },
                        Contacts = new List<Person>
                        {
                            new Person
                            {
                                Name = "Karmel",
                                Phone = 123456,
                            },
                            new Person
                            {
                                Name = "Danielle",
                                Phone = 9876543,
                            }
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        SupportCallNumber = 3,
                        Comments = new List<string>
                        {
                            "this was a good experiance", "thanks"
                        },
                        Contacts = new List<Person>
                        {
                            new Person
                            {
                                Name = "Yiftah",
                                Phone = 123456,
                            },
                            new Person
                            {
                                Name = "Grisha",
                                Phone = 9876543,
                            }
                        }
                    });

                    await session.SaveChangesAsync();
                }

                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Contacts.Select(contact => contact.Name).Any(name => name == "Grisha" || name == "Danielle")
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    var matches = new List<(string, int)> { ("James", 2) , ("Aviv", 3)};

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.True(matches.Remove((call.Name , call.SupportCallNumber)));
                    Assert.True(users.TryTake(out call, 5000));
                    Assert.True(matches.Remove((call.Name, call.SupportCallNumber)));
                    Assert.False(users.TryTake(out call, 50));

                }
            }
        }

        [Fact]
        public async Task CanHandleBooleanConstantsAndCount()
        {
            //RavenDB-7866
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall()
                    {
                        Name = "Michael",
                        Comments = new List<string>
                        {
                            "cool", "great", "nice", "special", "the best",
                            "fantastic", "awesome", "amazing", "life changing",
                            "unforgettable" , "a must", "don't miss", "once in a lifetime"
                        },
                        Votes = 15,
                        Survey = true
                    });

                    await session.StoreAsync(new SupportCall()
                    {
                        Name = "Maxim",
                        Comments = new List<string>
                        {
                            "cool", "great", "nice", "special", "the best",
                            "fantastic", "awesome", "amazing", "life changing",
                            "unforgettable" , "a must", "don't miss", "once in a lifetime"
                        },
                        Votes = 12,
                        Survey = false
                    });

                    await session.StoreAsync(new SupportCall()
                    {
                        Name = "Aviv",
                        Comments = new List<string>
                        {
                            "annoying"
                        },
                        Votes = 18,
                        Survey = false
                    });

                    await session.SaveChangesAsync();
                }

                var id = await store.Subscriptions.CreateAsync<SupportCall>(call =>
                    call.Comments.Count > 12 &&
                    call.Votes > 10 &&
                    call.Survey == false);

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Maxim", call.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        [Fact]
        public async Task CanHandleCountAsProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall()
                    {
                        Comments = new List<string>
                        {
                            "cool", "great", "nice", "special", "the best"
                        },
                        Person = new Person
                        {
                            Name = "Michael",
                            Count = 27
                        }
                    });

                    await session.StoreAsync(new SupportCall()
                    {
                        Comments = new List<string>
                        {
                            "unforgettable" , "a must", "don't miss", "once in a lifetime"
                        },
                        Person = new Person
                        {
                            Name = "Maxim",
                            Count = 17
                        }
                    });

                    await session.StoreAsync(new SupportCall()
                    {
                        Comments = new List<string>
                        {
                            "annoying"
                        },
                        Person = new Person
                        {
                            Name = "Aviv",
                            Count = 7
                        }
                    });

                    await session.SaveChangesAsync();
                }
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call =>
                        call.Comments.Count > 3 &&
                        call.Person.Count < 20
                        );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Maxim", call.Person.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        [Fact]
        public async Task CanHandleDates_Today()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Michael",
                        Started = new DateTime(2000, 1, 1)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Maxim",
                        Started = DateTime.Now.AddYears(1)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        Started = new DateTime(1986, 4, 19)
                    });

                    await session.SaveChangesAsync();
                }
                
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Started > DateTime.Today
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Maxim", call.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        [Fact]
        public async Task CanHandleDates_Now()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Michael",
                        Started = new DateTime(3000, 1, 1)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Maxim",
                        Started = DateTime.Now.AddYears(1)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        Started = new DateTime(1986, 4, 19)
                    });

                    await session.SaveChangesAsync();
                }
                

                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Started < DateTime.Now
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Aviv", call.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        [Fact]
        public async Task CanHandleDates_UtcNow()
        {
            using (var store = GetDocumentStore())
            {
                var utcTicks = DateTime.UtcNow.Ticks;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Michael",
                        Started = new DateTime(utcTicks).AddMinutes(-5)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Maxim",
                        Started = new DateTime(utcTicks).AddMinutes(-10)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        Started = new DateTime(utcTicks).AddMinutes(10)
                    });

                    await session.SaveChangesAsync();
                }
                
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Started > DateTime.UtcNow
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Aviv", call.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        [Fact]
        public async Task CanHandleDates_New()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Michael",
                        Started = new DateTime(2000, 1, 1)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Maxim",
                        Started = new DateTime(1983, 6, 6)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        Started = new DateTime(2017, 4, 19)
                    });

                    await session.SaveChangesAsync();
                }
                
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Started < new DateTime(1990, 1, 1)
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Maxim", call.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        [Fact]
        public async Task CanHandleDates_Year()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Michael",
                        Started = new DateTime(2000, 1, 1)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Maxim",
                        Started = new DateTime(1983, 6, 6)
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        Started = new DateTime(1942, 8, 1)
                    });

                    await session.SaveChangesAsync();
                }

                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Started.Year > 1999
                );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Michael", call.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        [Fact]
        public async Task CanHandleNestedDates()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Michael",
                        Person = new Person
                        {
                            DateOfBirth = new DateTime(1983 , 1, 1)
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Maxim",
                        Person = new Person
                        {
                            DateOfBirth = new DateTime(1985, 6, 6)
                        }
                    });

                    await session.StoreAsync(new SupportCall
                    {
                        Name = "Aviv",
                        Person = new Person
                        {
                            DateOfBirth = new DateTime(1986, 4, 19)
                        }
                    });

                    await session.SaveChangesAsync();
                }
                
                var id = await store.Subscriptions.CreateAsync<SupportCall>(
                    call => call.Person.DateOfBirth < new DateTime(1984, 1, 1)
                    );

                using (
                    var subscription =
                        store.Subscriptions.GetSubscriptionWorker<SupportCall>(new SubscriptionWorkerOptions(id) {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                        }))
                {
                    var users = new BlockingCollection<SupportCall>();

                    GC.KeepAlive(subscription.Run(supportCall =>
                    {
                        foreach (var item in supportCall.Items)
                        {
                            users.Add(item.Result);
                        }
                    }));

                    Assert.True(users.TryTake(out SupportCall call, 5000));
                    Assert.Equal("Michael", call.Name);
                    Assert.False(users.TryTake(out call, 50));
                }
            }
        }

        private class SupportCall
        {
            public string Name { get; set; }
            public int SupportCallNumber { get; set; }
            public List<string> Comments { get; set; }
            public List<Person> Contacts { get; set; }
            public int Votes { get; set; }
            public bool Survey { get; set; }
            public Person Person { get; set; }
            public DateTime Started { get; set; }
        }

        private class Node
        {
            public string Name { get; set; }
            public List<Node> Children { get; set; }
        }


        private class Person
        {
            public string Name { get; set; }
            public int Phone { get; set; }
            public long Count { get; set; }
            public DateTime DateOfBirth { get; set; }
        }
    }
}
