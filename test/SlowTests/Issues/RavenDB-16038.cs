using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16038 : RavenTestBase
    {
        public RavenDB_16038(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public void ShouldReturnLastModifiedInUtc()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Grisha"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        select new
                        {
                            Name = u.Name,
                            Metadata = RavenQuery.Metadata(u),
                            LastModified = RavenQuery.LastModified(u),
                        };

                    var list = query.ToList();

                    Assert.Equal(1, list.Count);

                    var lastModifiedFromProjection = list[0].Metadata.GetString(Constants.Documents.Metadata.LastModified);
                    Assert.Equal(DateTimeKind.Utc,list[0].LastModified.Kind);
                    DateTime dateTime = DateTime.ParseExact(lastModifiedFromProjection, "o", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
                    Assert.Equal(list[0].LastModified, dateTime.ToUniversalTime());
                }
            }
        }
    }
}
