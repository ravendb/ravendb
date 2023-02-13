using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_16276 : RavenTestBase
{
    public RavenDB_16276(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void SingleBucket_BatchBehavior_Will_Work_For_Commands_In_Same_Bucket_Store(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.Sharding.BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly;

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var company = new Company();

                session.Store(company, "companies/1");

                var order = new Order { Company = company.Id };

                session.Store(order, $"orders/1${company.Id}");

                session.SaveChanges();
            }
        }
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void SingleBucket_BatchBehavior_Will_Throw_For_Commands_In_Different_Bucket_Store(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.Sharding.BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly;

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var company = new Company();

                session.Store(company, "companies/1");

                var order = new Order { Company = company.Id };

                session.Store(order, "orders/1");

                Assert.Throws<ShardedBatchBehaviorViolationException>(() => session.SaveChanges());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void SingleBucket_BatchBehavior_Will_Not_Throw_For_Commands_In_Different_Bucket_Store_When_Cluster_Transaction_Is_Used(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.Sharding.BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly;

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var company = new Company();

                session.Store(company, "companies/1");

                var order = new Order { Company = company.Id };

                session.Store(order, "orders/1");

                session.SaveChanges();
            }
        }
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void SingleBucket_BatchBehavior_Will_Work_For_Commands_In_Same_Bucket_Patch(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.Sharding.BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly;

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Company, string>("companies/1", x => x.Name, "HR");
                session.Advanced.Patch<Order, string>("orders/1$companies/1", x => x.Employee, "E1");

                session.SaveChanges();
            }
        }
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void SingleBucket_BatchBehavior_Will_Throw_For_Commands_In_Different_Bucket_Patch(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.Sharding.BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly;

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Company, string>("companies/1", x => x.Name, "HR");
                session.Advanced.Patch<Order, string>("orders/1", x => x.Employee, "E1");

                Assert.Throws<ShardedBatchBehaviorViolationException>(() => session.SaveChanges());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void SingleBucket_BatchBehavior_Will_Work_For_Commands_In_Same_Bucket_PatchBatch(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.Sharding.BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly;

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var ids = new List<string>
                {
                    "companies/1",
                    "orders/1$companies/1"
                };

                session.Advanced.Defer(new BatchPatchCommandData(ids, new PatchRequest { Script = "" }, null));

                session.SaveChanges();
            }
        }
    }

    [RavenTheory(RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void SingleBucket_BatchBehavior_Will_Throw_For_Commands_In_Different_Bucket_PatchBatch(Options options)
    {
        options.ModifyDocumentStore = s => s.Conventions.Sharding.BatchBehavior = ShardedBatchBehavior.TransactionalSingleBucketOnly;

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var ids = new List<string>
                {
                    "companies/1",
                    "orders/1"
                };

                session.Advanced.Defer(new BatchPatchCommandData(ids, new PatchRequest { Script = "" }, null));

                Assert.Throws<ShardedBatchBehaviorViolationException>(() => session.SaveChanges());
            }
        }
    }
}
