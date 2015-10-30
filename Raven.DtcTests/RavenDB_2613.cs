// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2613.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Transactions;

using Raven.Tests.Helpers;

using Xunit;

namespace Raven.DtcTests
{
    public class RavenDB_2613 : RavenTestBase
    {
        [Fact]
        public void MaxNumberOfRequestsPerSessionShouldNotImpactDtc()
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: "esent"))
            {
                store.Conventions.MaxNumberOfRequestsPerSession = 2;

                var id = Guid.NewGuid().ToString();

                using (var session = store.OpenSession())
                {
                    session.Store(new TestClass { Id = id });
                    session.SaveChanges();
                }

                Console.WriteLine("Initial doc created");

                try
                {
                    using (var scope = new TransactionScope())
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Advanced.AllowNonAuthoritativeInformation = false;
                            var doc = session.Load<TestClass>(id);
                            doc.Blah++;
                            session.SaveChanges();
                        }

                        scope.Complete();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }

                Console.WriteLine("Retrieve initial document");
                using (var session = store.OpenSession())
                {
                    session.Advanced.AllowNonAuthoritativeInformation = false;
                    session.Load<TestClass>(id);
                }
            }
        }

        public class TestClass
        {
            public string Id { get; set; }

            public int Blah { get; set; }
        }
    }
}
