// -----------------------------------------------------------------------
//  <copyright file="AaronSt.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class ValerioBorioni : RavenTestBase
    {
        [Fact]
        public void RavenJValue_recognize_NAN_Float_isEqual_to_NAN_String()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new MyEntity());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.Query<MyEntity>().Customize(r=> r.WaitForNonStaleResults()).ToList();
                    var changes = session.Advanced.WhatChanged();
                    Assert.Empty(changes);
                }
            };

        }

        public class MyEntity
        {
            public double Value { get; set; }

            public MyEntity()
            {
                Value = double.NaN;
            }
        }

    }
}
