// -----------------------------------------------------------------------
//  <copyright file="ConcurrencyTests2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class ConcurrencyTests2 : RavenTest
    {

        public class Simple
        {
            public int Id;
            public string key;
            public int stamp;
        }

        [Fact]
        public void Concurrency_Passing_Test()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = false;
                    Simple simple = new Simple { Id = 1, key = "New", stamp = (int)DateTime.UtcNow.Ticks };
                    session.Store(simple);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = false;
                    Simple simple = new Simple { Id = 1, key = "Override", stamp = (int)DateTime.UtcNow.Ticks };
                    session.Store(simple);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void Concurrency_Passing_Test2()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    Simple simple = new Simple { Id = 1, key = "New", stamp = (int)DateTime.UtcNow.Ticks };
                    session.Store(simple);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = false;
                    Simple simple = new Simple { Id = 1, key = "Override", stamp = (int)DateTime.UtcNow.Ticks };
                    session.Store(simple);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void Concurrency_Failing_Test()
        {
            using (var store = NewRemoteDocumentStore())
            {
                for (int i = 0; i < 2; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Advanced.UseOptimisticConcurrency = true;
                        Simple simple = new Simple { Id = 1, key = "New", stamp = (int)DateTime.UtcNow.Ticks };
                        session.Store(simple);
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Advanced.UseOptimisticConcurrency = false;
                        Simple simple = new Simple { Id = 1, key = "Override", stamp = (int)DateTime.UtcNow.Ticks };
                        session.Store(simple);
                        session.SaveChanges();
                    }
                }
            }
        }
    }
}