// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1609.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Connection;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;
using Raven.Client;

namespace Raven.Tests.Issues
{ 
    public class RavenDB_1609 : RavenTest
    {
        [Fact]
        public void CanDisplayLazyRequestTimes_Remote()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Test User #1" }, "users/1");
                    session.Store(new User { Name = "Test User #2" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Lazily.Load<User>("users/1");
                    session.Advanced.Lazily.Load<User>("users/2");
                    session.Query<User>().Lazily();

                    var requestTimes = session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(requestTimes.TotalClientDuration);
                    Assert.NotNull(requestTimes.TotalServerDuration);
                    Assert.Equal(3, requestTimes.DurationBreakdown.Count);

                }
            }
        }

        [Fact]
        public void CanDisplayLazyRequestTimes_Embedded()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Test User #1" }, "users/1");
                    session.Store(new User { Name = "Test User #2" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Lazily.Load<User>("users/1");
                    session.Advanced.Lazily.Load<User>("users/2");
                    session.Query<User>().Lazily();

                    var requestTimes = session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(requestTimes.TotalClientDuration);
                    Assert.NotNull(requestTimes.TotalServerDuration);
                    Assert.Equal(3, requestTimes.DurationBreakdown.Count);

                }
            }
        }

        [Fact]
        public void CanProperlyParseTotalTime()
        {
            long result;
            SerializationHelper.TryParseTempRequestTime("1,573", out result);
            Assert.Equal(1573, result);
        }
    }
}