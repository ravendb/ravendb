using System;
using System.Collections.Generic;
using System.Threading;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    [Trait("Category", "ETL")]
    public class EtlTestBase : RavenTestBase
    {
        protected static void SetupEtl(DocumentStore src, EtlDestinationsConfig configuration)
        {
            using (var session = src.OpenSession())
            {
                session.Store(configuration, Constants.Documents.ETL.RavenEtlDocument);

                session.SaveChanges();
            }
        }

        protected static void SetupEtl(DocumentStore src, DocumentStore dst, string collection, string script)
        {
            SetupEtl(src, dst, new[] { collection }, script);
        }

        protected static void SetupEtl(DocumentStore src, DocumentStore dst, IEnumerable<string> collections, string script)
        {
            SetupEtl(src, new EtlDestinationsConfig
            {
                RavenDestinations =
                {
                    new EtlConfiguration<RavenDestination>()
                    {
                        Destination = new RavenDestination
                        {
                            Database = dst.DefaultDatabase,
                            Url = dst.Url
                        },
                        Transforms =
                        {
                            new Transformation
                            {
                                Name = $"{src} to {dst}",
                                Collections = new List<string>(collections),
                                Script = script
                            }
                        }
                    }

                }
            });
        }

        protected ManualResetEventSlim WaitForEtl(DocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var database = GetDatabase(store.DefaultDatabase).Result;

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