// -----------------------------------------------------------------------
//  <copyright file="ChangeTrackingDoesntWorkForDecimalsWithSmallChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class ChangeTrackingDoesntWorkForDecimalsWithSmallChanges : RavenTest
    {
        public class TestClass
        {
            public string Id { get; set; }
            public decimal ADecimalValue { get; set; }
        }

        [Fact]
        public void BigChangeWorks()
        {
            // this goes OK
            ChangeADecimalValue(11111111.11m, 21111112.11m);
        }

        [Fact]
        public void SmallChangeDoesntWork()
        {
            // this borks
            ChangeADecimalValue(11111111.11m, 11111122.11m);
        }

        /// <summary>
        /// - Create a test class, set ADecimalValue to '@from' and save it to RavenDB
        /// - Load the instance in a new session, change the value to 'to' and save changes
        /// - Load the instance in a new session and verify that in the previous stap ADecimalValue was actually changed
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        private void ChangeADecimalValue(decimal @from, decimal to)
        {
            using (var store = NewDocumentStore())
            {
                string id;
                using (var ses = store.OpenSession())
                {
                    var ding = new TestClass { ADecimalValue = @from };
                    ses.Store(ding);
                    ses.SaveChanges();
                    id = ding.Id;
                }
                using (var ses = store.OpenSession())
                {
                    var ding = ses.Load<TestClass>(id);
                    ding.ADecimalValue = to;
                    ses.SaveChanges();
                }
                using (var ses = store.OpenSession())
                {
                    var ding = ses.Load<TestClass>(id);
                    Assert.Equal(to, ding.ADecimalValue);
                }
            }
        }
    }
}