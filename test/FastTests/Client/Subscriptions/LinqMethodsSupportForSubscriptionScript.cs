using Raven.Client.Documents.Subscriptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class LinqMethodsSupportForSubscriptionScript : RavenTestBase
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

                var subscriptionCreationParams = new SubscriptionCreationOptions<SupportCall>
                {
                    Criteria = new SubscriptionCriteria<SupportCall>(call => call.Name == "James" && call.Comments.Any(comment => comment.Contains("annoying")))
                };

                Assert.Equal("return this.Name===\"James\"&&this.Comments.some(function(comment){return comment.indexOf(\"annoying\")>=0;});",
                        subscriptionCreationParams.Criteria.Script);

                var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var subscription =
                        store.Subscriptions.Open<SupportCall>(new SubscriptionConnectionOptions(id)))
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

                var subscriptionCreationParams = new SubscriptionCreationOptions<SupportCall>
                {
                    Criteria = new SubscriptionCriteria<SupportCall>(call => call.Name == "James" && call.Comments.All(comment => comment.Contains("nice")))
                };

                Assert.Equal("return this.Name===\"James\"&&this.Comments.every(function(comment){return comment.indexOf(\"nice\")>=0;});",
                    subscriptionCreationParams.Criteria.Script);

                var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var subscription =
                        store.Subscriptions.Open<SupportCall>(new SubscriptionConnectionOptions(id)))
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

                var subscriptionCreationParams = new SubscriptionCreationOptions<SupportCall>
                {
                    //all of the SupportCalls where all of the comments that contain the word "nice" also contain the word "very"
                    Criteria = new SubscriptionCriteria<SupportCall>(call => call.Comments.Where(comment => comment.Contains("nice")).All(comment => comment.Contains("very")))
                };

                Assert.Equal("return this.Comments.filter(function(comment){return comment.indexOf(\"nice\")>=0;}).every(function(comment){return comment.indexOf(\"very\")>=0;});",
                    subscriptionCreationParams.Criteria.Script);

                var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var subscription =
                        store.Subscriptions.Open<SupportCall>(new SubscriptionConnectionOptions(id)))
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

                var subscriptionCreationParams = new SubscriptionCreationOptions<SupportCall>
                {
                    //all of the SupportCalls where there is a contact named "Grisha" or "Danielle"
                    Criteria = new SubscriptionCriteria<SupportCall>(call => call.Contacts.Select(contact => contact.Name).Any(name => name == "Grisha" || name == "Danielle"))
                };

                Assert.Equal("return this.Contacts.map(function(contact){return contact.Name;}).some(function(name){return name===\"Grisha\"||name===\"Danielle\";});",
                    subscriptionCreationParams.Criteria.Script);

                var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var subscription =
                        store.Subscriptions.Open<SupportCall>(new SubscriptionConnectionOptions(id)))
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

                var options = new SubscriptionCreationOptions<SupportCall>
                {
                    Criteria = new SubscriptionCriteria<SupportCall>(
                        call =>
                            call.Comments.Count > 12 &&
                            call.Votes > 10 &&
                            call.Survey == false
                    )
                };

                Assert.Equal("return this.Comments.length>12&&this.Votes>10&&this.Survey===false;",
                    options.Criteria.Script);

                var id = await store.Subscriptions.CreateAsync(options);

                using (
                    var subscription =
                        store.Subscriptions.Open<SupportCall>(new SubscriptionConnectionOptions(id)))
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

                var options = new SubscriptionCreationOptions<SupportCall>
                {
                    Criteria = new SubscriptionCriteria<SupportCall>(
                        call =>
                            call.Comments.Count > 3 &&
                            call.Person.Count < 20
                    )
                };

                Assert.Equal("return this.Comments.length>3&&this.Person.Count<20;",
                    options.Criteria.Script);

                var id = await store.Subscriptions.CreateAsync(options);

                using (
                    var subscription =
                        store.Subscriptions.Open<SupportCall>(new SubscriptionConnectionOptions(id)))
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

        private class SupportCall
        {
            public string Name { get; set; }
            public int SupportCallNumber { get; set; }
            public List<string> Comments { get; set; }
            public List<Person> Contacts { get; set; }
            public int Votes { get; set; }
            public bool Survey { get; set; }
            public Person Person { get; set; }

        }

        private class Person
        {
            public string Name { get; set; }
            public int Phone { get; set; }
            public long Count { get; set; }
        }
    }
}
