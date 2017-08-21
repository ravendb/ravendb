using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ETL;
using Raven.Server.Documents.ETL;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    [Trait("Category", "ETL")]
    public abstract class EtlTestBase : RavenTestBase
    {
        protected static AddEtlOperationResult AddEtl<T>(DocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
        {
            src.Admin.Server.Send(new PutConnectionStringOperation<T>(connectionString, src.Database));
            return src.Admin.Server.Send(new AddEtlOperation<T>(configuration, src.Database));
        }

        protected static AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false)
        {
            return AddEtl(src, dst, new[] { collection }, script, applyToAllDocuments, disabled);
        }

        protected static AddEtlOperationResult AddEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script, bool applyToAllDocuments = false, bool disabled = false)
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
                    }
                },
                new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dst.Database,
                    Url = dst.Urls.First(),
                }
            );
        }

        protected ManualResetEventSlim WaitForEtl(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var database = GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.EtlLoader.BatchCompleted += (n, s) =>
            {
                if (predicate(n, s))
                    mre.Set();
            };

            return mre;
        }
    }
}