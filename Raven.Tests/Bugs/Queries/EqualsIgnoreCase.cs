using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Client;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Xunit;
using Raven.Database.Data;
using Raven.Client;
using System.IO;
using Raven.Client.Document;
using Raven.Client.Linq;
using System.Threading;
using System.Diagnostics;

namespace Raven.Tests.Bugs.Queries
{
    public class EqualsIgnoreCase : LocalClientTest
    {
        [Fact]
        public void QueryWithEquals_StringWithDifferentCasing_ReturnsFalse()
        {
            using (var db = NewDocumentStore())
            {
                db.Initialize();

                using (var session = db.OpenSession())
                {
                    session.Store(new User() { Name = "Matt" });
                    session.SaveChanges();

                    bool testQuery;

                    Assert.Equal(1, session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count());
                    Assert.False(string.Equals("Matt", "matt"));

                    // Equals w/o InvariantCultureIgnoreCase
                    testQuery = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Any(x => x.Name.Equals("matt"));

                    Assert.False(testQuery);
                }
            }
        }

        [Fact]
        public void QueryWithEquals_StringWithSameCasing_ReturnsTrue()
        {
            using (var db = NewDocumentStore())
            {
                db.Initialize();

                using (var session = db.OpenSession())
                {
                    session.Store(new User() { Name = "Matt" });
                    session.SaveChanges();

                    bool testQuery;

                    Assert.Equal(1, session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count());
                    Assert.True(string.Equals("Matt", "Matt"));

                    // Equals w/o InvariantCultureIgnoreCase
                    testQuery = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Any(x => x.Name.Equals("Matt"));

                    Assert.True(testQuery);

                }
            }
        }

        [Fact]
        public void QueryWithEqualsInvariantCultureIgnoreCase_StringWithDifferentCasing_ReturnsTrue()
        {
            using (var db = NewDocumentStore())
            {
                db.Initialize();

                using (var session = db.OpenSession())
                {
                    session.Store(new User() { Name = "Matt" });
                    session.SaveChanges();

                    bool testQuery;

                    Assert.Equal(1, session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count());
                    Assert.True(string.Equals("Matt", "matt", StringComparison.InvariantCultureIgnoreCase));

                    // Equals with InvariantCultureIgnoreCase
                    testQuery = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Any(x => x.Name.Equals("matt", StringComparison.InvariantCultureIgnoreCase));

                    Assert.True(testQuery);

                }
            }
        }

        [Fact]
        public void QueryWithEqualsInvariantCultureIgnoreCaseUsingAny_StringWithSameCasing_ReturnsTrue()
        {
            using (var db = NewDocumentStore())
            {
                db.Initialize();

                using (var session = db.OpenSession())
                {
                    session.Store(new User() { Name = "Matt" });
                    session.SaveChanges();

                    bool testQuery;

                    Assert.Equal(1, session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count());
                    Assert.True(string.Equals("Matt", "Matt", StringComparison.InvariantCultureIgnoreCase));

                    // Equals with InvariantCultureIgnoreCase
                    testQuery = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Any(x => x.Name.Equals("Matt", StringComparison.InvariantCultureIgnoreCase));

                    Assert.True(testQuery);

                }
            }
        }

        [Fact]
        public void QueryWithEqualsInvariantCultureIgnoreCaseUsingCount_StringWithSameCasing_ReturnsTrue()
        {
            using (var db = NewDocumentStore())
            {
                db.Initialize();

                using (var session = db.OpenSession())
                {
                    session.Store(new User() { Name = "Matt" });
                    session.SaveChanges();

                    bool testQuery;

                    Assert.Equal(1, session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count());
                    Assert.True(string.Equals("Matt", "Matt", StringComparison.InvariantCultureIgnoreCase));

                    // Equals with InvariantCultureIgnoreCase
                    testQuery = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Count(x => x.Name.Equals("Matt", StringComparison.InvariantCultureIgnoreCase))==1;

                    Assert.True(testQuery);

                }
            }
        }

        [Fact]
        public void QueryWithEqualsInvariantCultureIgnoreCaseUsingWhere_StringWithSameCasing_ReturnsTrue()
        {
            using (var db = NewDocumentStore())
            {
                db.Initialize();

                using (var session = db.OpenSession())
                {
                    session.Store(new User() { Name = "Matt" });
                    session.SaveChanges();

                    bool testQuery;

                    Assert.Equal(1, session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count());
                    Assert.True(string.Equals("Matt", "Matt", StringComparison.InvariantCultureIgnoreCase));

                    // Equals with InvariantCultureIgnoreCase
                    testQuery = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name.Equals("Matt", StringComparison.InvariantCultureIgnoreCase))
                            .ToArray().Count()== 1;

                    Assert.True(testQuery);

                }
            }
        }
    }
}
