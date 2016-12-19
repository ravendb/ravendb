using Raven.Abstractions.Data;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Xunit;
using Raven.Tests.Common;
using System.Collections.Concurrent;
using Xunit.Extensions;

namespace Raven.Tests.Subscriptions
{
    public class SubscriptionsPaths:RavenTest
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);
        public class Node
        {
            public string Name { get; set; }
            public List<Node> Children = new List<Node>();
        }

        [Theory]
        [PropertyData("Storages")]
        public void PositivePathWithCollectionsTyped(string storage)
        {
            var nestedNode = new Node
            {
                Name = "Parent",
                Children = Enumerable.Range(0, 10).Select(x => new Node()
                {
                    Name = "Child" + x,
                    Children = Enumerable.Range(0, 5).Select(y => new Node()
                    {
                        Name = "Grandchild",
                        Children = null
                    }).ToList()
                }).ToList()
            };

            var simpleNode = new Node
            {
                Name = "ChildlessParent",
                Children = null
            };
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }				
                Expression<Func<Node, object>> expr = node => node.Children.SelectMany(x => x.Children).Select(x => x.Name);
                var subscriptionID = store.Subscriptions.Create<Node>(new SubscriptionCriteria<Node>
                {
                    PropertiesMatch = new Dictionary<Expression<Func<Node, object>>, RavenJToken>()
                    {				
                        {node => node.Children.SelectMany(x => x.Children).Select(x => x.Name), "Grandchild"}
                    }
                });

                var subscription = store.Subscriptions.Open<Node>(subscriptionID, new SubscriptionConnectionOptions());
                
                var keys = new BlockingCollection<Node>();
                subscription.Subscribe(x =>
                {
                    keys.Add(x);
                });
                
                Node key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));				
                Assert.False(keys.TryTake(out key, TimeSpan.FromSeconds(1)));
                subscription.Dispose();
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PositivePathWithCollectionsUntyped(string storage)
        {
            var nestedNode = new Node
            {
                Name = "Parent",
                Children = Enumerable.Range(0, 10).Select(x => new Node()
                {
                    Name = "Child" + x,
                    Children = Enumerable.Range(0, 5).Select(y => new Node()
                    {
                        Name = "Grandchild",
                        Children = null
                    }).ToList()
                }).ToList()
            };

            var simpleNode = new Node
            {
                Name = "ChildlessParent",
                Children = null
            };
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }
                
                var subscriptionID = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    PropertiesMatch = new Dictionary<string, RavenJToken>()
                    {
                        {"Children,Children,Name", "Grandchild"}
                    }
                });

                var subscription = store.Subscriptions.Open(subscriptionID, new SubscriptionConnectionOptions());

                var keys = new BlockingCollection<object>();
                subscription.Subscribe(x =>
                {
                    keys.Add(x);
                });

                object key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                Assert.False(keys.TryTake(out key, TimeSpan.FromSeconds(1)));
                subscription.Dispose();
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void NegativePathWithCollectionsTyped(string storage)
        {
            var nestedNode = new Node
            {
                Name = "Parent",
                Children = Enumerable.Range(0, 10).Select(x => new Node()
                {
                    Name = "Child" + x,
                    Children = Enumerable.Range(0, 5).Select(y => new Node()
                    {
                        Name = "Grandchild",
                        Children = null
                    }).ToList()
                }).ToList()
            };

            var simpleNode = new Node
            {
                Name = "ChildlessParent",
                Children = null
            };
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }
                var subscriptionID = store.Subscriptions.Create<Node>(new SubscriptionCriteria<Node>
                {

                    PropertiesNotMatch = new Dictionary<Expression<Func<Node, object>>, RavenJToken>()
                    {						
                        {node => node.Children.SelectMany(x => x.Children).Select(x => x.Name), "Parent"}
                    }
                });

                var subscription = store.Subscriptions.Open<Node>(subscriptionID, new SubscriptionConnectionOptions());

                var keys = new BlockingCollection<Node>();
                subscription.Subscribe(x =>
                {
                    keys.Add(x);
                });

                Node key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                subscription.Dispose();
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void NegativePathWithCollectionsUntyped(string storage)
        {
            var nestedNode = new Node
            {
                Name = "Parent",
                Children = Enumerable.Range(0, 10).Select(x => new Node()
                {
                    Name = "Child" + x,
                    Children = Enumerable.Range(0, 5).Select(y => new Node()
                    {
                        Name = "Grandchild",
                        Children = null
                    }).ToList()
                }).ToList()
            };

            var simpleNode = new Node
            {
                Name = "ChildlessParent",
                Children = null
            };
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }
                var subscriptionID = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    PropertiesNotMatch = new Dictionary<string, RavenJToken>()
                    {
                        {"Children,Children,Name", "Parent"}
                    }
                });

                var subscription = store.Subscriptions.Open(subscriptionID, new SubscriptionConnectionOptions());

                var keys = new BlockingCollection<object>();
                subscription.Subscribe(x =>
                {
                    keys.Add(x);
                });

                object key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                subscription.Dispose();
            }
        }
    }
}
