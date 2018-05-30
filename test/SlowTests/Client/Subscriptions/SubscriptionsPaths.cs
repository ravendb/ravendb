using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class SubscriptionsPaths : RavenTestBase
    {
        public class Node
        {
            public string Name { get; set; }
            public List<Node> Children = new List<Node>();
        }

        [Fact]
        public void PositivePathWithCollectionsTyped()
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }
                Expression<Func<Node, object>> expr = node => node.Children.SelectMany(x => x.Children).Select(x => x.Name);
                var subscriptionID = store.Subscriptions.Create<Node>(
                        node => node.Children.SelectMany(x => x.Children).All(x => x.Name == "Grandchild"));

                var subscription = store.Subscriptions.GetSubscriptionWorker<Node>(subscriptionID);

                var keys = new BlockingCollection<Node>();
                subscription.Run(x => x.Items.ForEach(i => keys.Add(i.Result)));

                Node key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                Assert.False(keys.TryTake(out key, TimeSpan.FromSeconds(1)));
                subscription.Dispose();
            }
        }

        [Fact]
        public void PositivePathWithCollectionsUntyped()
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }

                var subscriptionID = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"
declare function areAllGrandchildsGrandchilds(doc){
        return doc.Children.every(function (child) { 
            return child.Children.every(function (grandchild){ 
                return grandchild.Name == 'Grandchild'
            });
        });
}

From Nodes as n Where areAllGrandchildsGrandchilds(n)"
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionID) {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var keys = new BlockingCollection<object>();
                subscription.Run(x => x.Items.ForEach(i => keys.Add(i.Result)));

                object key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                Assert.False(keys.TryTake(out key, TimeSpan.FromSeconds(1)));
                subscription.Dispose();
            }
        }

        [Fact]
        public void NegativePathWithCollectionsTyped()
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }
                var subscriptionID = store.Subscriptions.Create<Node>(node => 
                node.Children.All(x => 
                    x.Children.All(i => i.Name == "Grandchild")));

                var subscription = store.Subscriptions.GetSubscriptionWorker<Node>(new SubscriptionWorkerOptions(subscriptionID));

                var keys = new BlockingCollection<Node>();
                subscription.Run(x => x.Items.ForEach(i => keys.Add(i.Result)));

                Node key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                Assert.False(keys.TryTake(out key, TimeSpan.FromSeconds(1)));
                subscription.Dispose();
            }
        }

        [Fact]
        public void NegativePathWithCollectionsUntyped()
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(nestedNode);
                    session.Store(simpleNode);
                    session.SaveChanges();
                }
         
                var subscriptionID = store.Subscriptions.Create(new SubscriptionCreationOptions()
                {
                    Query = @"declare function areAllGrandchildsGrandchilds(doc){

        if (!doc.Children)
            return true;

        return doc.Children.every(function (child) { 
            return child.Children.every(function (grandchild){ 
                return grandchild.Name != 'Parent'
            });
        });
}

From Nodes as n Where areAllGrandchildsGrandchilds(n)"

                });


                var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionID));

                var keys = new BlockingCollection<object>();
                subscription.Run(x => x.Items.ForEach(i => keys.Add(i.Result)));

                object key;
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                Assert.True(keys.TryTake(out key, TimeSpan.FromSeconds(20)));
                subscription.Dispose();
            }
        }
    }
}
