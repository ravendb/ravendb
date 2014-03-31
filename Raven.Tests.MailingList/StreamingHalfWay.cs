// -----------------------------------------------------------------------
//  <copyright file="StreamingHalfWay.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class StreamingHalfWay : RavenTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                CreateSomeData(store);
                using (var session = store.OpenSession())
                {
                    using (var enumerator = session.Advanced.Stream<dynamic>("foos/"))
                    {
                        enumerator.MoveNext(); // should allow to dispose & move on
                    }
                }
            }   
        }

        public class Foo
        {
            public string Id { get; set; }
        }
        private static void CreateSomeData(IDocumentStore store)
        {
            for (int k = 0; k < 10; k++)
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1024; i++)
                    {
                        session.Store(new Foo());
                    }
                    session.SaveChanges();
                }
            }
        }
    }
}