// -----------------------------------------------------------------------
//  <copyright file="Kamran.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class Kamran : RavenTest
    {
        public class Foo
        {
            public string Name;
        }

        [Fact]
        public void CanUseEscapeInQueries()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    Assert.Equal("Name:(\\\"Foo\\\"\\: Test)", s.Query<Foo>()
                        .Search(x => x.Name , "\"Foo\": Test").ToString());
                    Assert.Equal("Name:\"\\\"Foo\\\": Test\"", s.Query<Foo>()
                        .Where(x => x.Name == "\"Foo\": Test").ToString());
                }
            }
        }
    }
}