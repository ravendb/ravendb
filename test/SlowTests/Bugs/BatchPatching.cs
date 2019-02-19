using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Bugs
{
    public class BatchPatching : RavenTestBase
    {
        [Fact]
        public void CanSuccessfullyPatchInBatches()
        {
            using (var store = GetDocumentStore())
            {
                const int count = 512;
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        s.Store(new User
                        {
                            Age = i,
                        }, "users/" + i);
                    }
                    s.SaveChanges();
                }

                var batchesFirstHalf =
                    Enumerable.Range(0, count / 2).Select(i => new PatchOperation("users/" + i, null, new PatchRequest
                    {
                        Script = $"this.Name='Users-{i}';"
                    }));
                foreach (var patchCommandData in batchesFirstHalf)
                {
                    if (store.Operations.Send(patchCommandData) != PatchStatus.Patched)
                        throw new InvalidOperationException("Some patches failed");
                }

                var batchesSecondHalf =
                    Enumerable.Range(count / 2, count / 2).Select(i => new PatchOperation("users/" + i, null, new PatchRequest
                    {
                        Script = $"this.Name='Users-{i}';"
                    }));
                foreach (var patchCommandData in batchesSecondHalf)
                {
                    if (store.Operations.Send(patchCommandData) != PatchStatus.Patched)
                        throw new InvalidOperationException("Some patches failed");
                }

                using (var s = store.OpenSession())
                {
                    s.Advanced.MaxNumberOfRequestsPerSession = count + 2;
                    for (int i = 0; i < count; i++)
                    {
                        Assert.Equal("Users-" + i, s.Load<User>("users/" + i).Name);
                    }
                }

            }
        }
    }
}
