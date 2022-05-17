using Tests.Infrastructure;
using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class BatchPatching : RavenTestBase
    {
        public BatchPatching(ITestOutputHelper output) : base(output)
        {
        }

        //[Theory]
        //[RavenData(10_000, 0, 0)]
        //[RavenData(1000, 2, 2)]
        //[RavenData(1000, 10, 10)]
        //[RavenData(1000, 10, 25)]
        //[RavenData(10_000, 10, 25)]
        ////[InlineData(1000, "V8", 10, 50)]
        ////[InlineData(10_000, "V8", 10, 50)]
        ////[InlineData(1000, "V8", 10, 250)]
        ////[InlineData(2000, "V8", 3, 500)]
        ////[InlineData(5_000, "V8", 4, 1000)]
        //public void CanSuccessfullyPatchInBatches(int count, int targetContextCountPerEngine, int maxEngineCount)
        //{
        //[Theory]
        //[RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        //public void CanSuccessfullyPatchInBatches(Options options)
        //{
        //    //var options = Options.ForJavaScriptEngine(jsEngineType, d =>
        //    //{
        //    //    d.Settings[RavenConfiguration.GetKey(x => x.JavaScript.TargetContextCountPerEngine)] = targetContextCountPerEngine.ToString();            
        //    //    d.Settings[RavenConfiguration.GetKey(x => x.JavaScript.MaxEngineCount)] = maxEngineCount.ToString();            
        //    //});
        //    using (var store = GetDocumentStore(options))
        //    {
        //        // 2406 V8 isolates max count achieved, 2415 failed
        //        using (var s = store.OpenSession())
        //        {
        //            for (int i = 0; i < count; i++)
        //            {
        //                s.Store(new User
        //                {
        //                    Age = i,
        //                }, "users/" + i);
        //            }
        //            s.SaveChanges();
        //        }

        //        var batchesFirstHalf =
        //            Enumerable.Range(0, count / 2).Select(i => new PatchOperation("users/" + i, null, new PatchRequest
        //            {
        //                Script = $"if (this) {{ this.Name='Users-{i}'; }}"
        //            }));
        //        foreach (var patchCommandData in batchesFirstHalf)
        //        {
        //            if (store.Operations.Send(patchCommandData) != PatchStatus.Patched)
        //                throw new InvalidOperationException("Some patches failed");
        //        }

        //        var batchesSecondHalf =
        //            Enumerable.Range(count / 2, count / 2).Select(i => new PatchOperation("users/" + i, null, new PatchRequest
        //            {
        //                Script = $"if (this) {{ this.Name='Users-{i}'; }}"
        //            }));
        //        foreach (var patchCommandData in batchesSecondHalf)
        //        {
        //            if (store.Operations.Send(patchCommandData) != PatchStatus.Patched)
        //                throw new InvalidOperationException("Some patches failed");
        //        }

        //        using (var s = store.OpenSession())
        //        {
        //            s.Advanced.MaxNumberOfRequestsPerSession = count + 2;
        //            for (int i = 0; i < count; i++)
        //            {
        //                Assert.Equal("Users-" + i, s.Load<User>("users/" + i).Name);
        //            }
        //        }

        //    }
        //}

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanSuccessfullyPatchInBatches(Options options)
        {
            using (var store = GetDocumentStore(options))
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
