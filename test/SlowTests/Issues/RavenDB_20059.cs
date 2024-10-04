using System;
using FastTests;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.NotificationCenter.Notifications.Details;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20059 : RavenTestBase
    {
        public RavenDB_20059(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanModifyGlobalObjectUsingEtlTransformScriptWithDeleteDocumentsBehaviorFunction(Options options)
        {
            using (var srcStore = GetDocumentStore(options))
            using (var destStore = GetDocumentStore())
            {
                Etl.AddEtl(srcStore, destStore, "Contracts", script:
@"this.Contact.AdditionalInfo = 13;	
loadToContractsTemp(this);
function deleteDocumentsOfContractsBehavior(docId) {
    return false;
    }");

                var etlDone = Etl.WaitForEtlToComplete(srcStore);

                using (var session = srcStore.OpenSession())
                {
                    session.Store(new Contract { Contact = new Contact { AdditionalInfo = 10 } });

                    session.SaveChanges();
                }
                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = destStore.OpenSession())
                {
                    var contract = session.Load<Contract>("contracts/1-A/ContractsTemp/0000000000000000001-A");
                    Assert.NotNull(contract);
                    Assert.Equal(13, contract.Contact.AdditionalInfo);
                }

                using (var session = srcStore.OpenSession())
                {
                    session.Delete("contracts/1-A");
                    session.SaveChanges();
                }

                etlDone = Etl.WaitForEtlToComplete(srcStore);
                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = destStore.OpenSession())
                {
                    var contract = session.Load<Contract>("contracts/1-A/ContractsTemp/0000000000000000001-A");

                    Assert.NotNull(contract);
                    Assert.Equal(13, contract.Contact.AdditionalInfo);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public void WeStillShouldGetErrorWhenEtlProcessRunsBehaviorFunctionWithInvalidSyntax()
        {
            using (var srcStore = GetDocumentStore())
            using (var destStore = GetDocumentStore())
            {
                var script =
@"this.Contact = .;	
loadToContractsTemp(this);
function deleteDocumentsOfContractsBehavior(docId) {
    return false;
    }";
                Etl.AddEtl(srcStore, destStore, new[] { "Contracts" }, script, out var config);

                using (var session = srcStore.OpenSession())
                {
                    session.Store(new Contract { Contact = new Contact { AdditionalInfo = 10 } });
                    session.SaveChanges();
                }

                var timeout = (int)TimeSpan.FromSeconds(15).TotalMilliseconds;

                EtlErrorInfo error = null;
                Assert.True(WaitForValue(() => Etl.TryGetTransformationError(srcStore.Database, config, out error), true, timeout: timeout));

                Assert.NotNull(error);
                Assert.True(error.Error.Contains($"{nameof(JavaScriptParseException)}: Failed to parse:"));
                Assert.True(error.Error.Contains("Unexpected token '.'"));
            }
        }

        internal class Contract
        {
            public Contact Contact { get; init; }
        }

        internal class Contact
        {
            public int AdditionalInfo { get; set; }
        }
    }
}
