using System;
using System.Globalization;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16033 : RavenTestBase
    {
        public RavenDB_16033(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanSelectMetadata_SingleCallExpression(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                const string id = "users/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);
                    session.SaveChanges();
                }

                DateTime? lastModified;
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    lastModified = session.Advanced.GetLastModifiedFor(user);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => RavenQuery.Metadata(u));
                    var metadata = query.First();

                    Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.LastModified, out string lm));
                    DateTime lastModifiedFromProjection = DateTime.ParseExact(lm, "o", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUniversalTime();
                    Assert.Equal(lastModified, lastModifiedFromProjection);
                }
            }
        }
    }
}
