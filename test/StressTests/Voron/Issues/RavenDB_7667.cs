using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron.Issues
{
    public class RavenDB_7667 : RavenTestBase
    {
        public RavenDB_7667(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void IndexTreeNeedsToReturnPageToAllocatorInsteadOfFreeSpaceHandling()
        {
            using (var store = GetDocumentStore())
            {
                const int documentsCount = 150000;
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < documentsCount; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            FirstName = RandomString(30),
                            LastName = RandomString(30),
                            Phone = RandomString(30)
                        });
                    }
                }

                store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM Users" })).WaitForCompletion(TimeSpan.FromMinutes(5));
            }
        }

        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string Phone { get; set; }
        }

        private static readonly Random Random = new Random(1);
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[Random.Next(s.Length)]).ToArray());
        }
    }
}
