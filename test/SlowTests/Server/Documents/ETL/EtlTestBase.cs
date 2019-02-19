using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    [Trait("Category", "ETL")]
    public abstract class EtlTestBase : RavenTestBase
    {
        protected static AddEtlOperationResult AddEtl<T>(DocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
        {
            src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            return src.Maintenance.Send(new AddEtlOperation<T>(configuration));
        }

        protected static AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            return AddEtl(src, dst, new[] { collection }, script, applyToAllDocuments, disabled, mentor);
        }

        protected static AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dst.Database}@{dst.Urls.First()}";

            return AddEtl(src, new RavenEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(collections),
                            Script = script,
                            ApplyToAllDocuments = applyToAllDocuments,
                            Disabled = disabled
                        }
                    },
                    MentorNode = mentor
                },
                new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dst.Database,
                    TopologyDiscoveryUrls = dst.Urls,
                }
            );
        }

        protected ManualResetEventSlim WaitForEtl(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var database = GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };

            return mre;
        }
    }
}
