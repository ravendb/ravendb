// -----------------------------------------------------------------------
//  <copyright file="EntityWithStack.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class EntityWithStack : RavenTest
    {

        public class Order
        {
            public Order()
            {
                Statuses = new Stack<string>();
            }
            public string Id { get; set; }
            public Stack<string> Statuses { get; set; }
        }

        [Fact]
        public void CanWork()
        {
             string id;
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var doc = new Order();
                    session.Store(doc);
                    session.SaveChanges();
                    id = doc.Id;
                    session.Load<Order>(id);
                }

                using (var session = documentStore.OpenSession())
                {
                    session.Load<Order>(id); // throws an exception
                }
            }
        }
    }
}