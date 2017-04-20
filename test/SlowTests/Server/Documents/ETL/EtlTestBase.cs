using System;
using System.Collections.Generic;
using System.Threading;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;

namespace SlowTests.Server.Documents.ETL
{
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
            using (var session = src.OpenSession())
            {
                session.Store(new EtlDestinationsConfig()
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
                                    Collections = { collection },
                                    Script = script
                                }
                            }
                        }
                        
                    }
                }, Constants.Documents.ETL.RavenEtlDocument);

                session.SaveChanges();
            }
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