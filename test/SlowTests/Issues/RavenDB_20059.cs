using System;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.NotificationCenter.Notifications.Details;
using SlowTests.Server.Documents.ETL;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20059 : EtlTestBase
    {
        public RavenDB_20059(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanModifyGlobalObjectUsingEtlTransformScriptWithDeleteDocumentsBehaviorFunction()
        {
            using (var srcStore = GetDocumentStore())
            using (var destStore = GetDocumentStore())
            {
                AddEtl(srcStore, destStore, "Contracts", script:
@"this.Contact.AdditionalInfo = 13;	
loadToContractsTemp(this);
function deleteDocumentsOfContractsBehavior(docId) {
    return false;
    }");

                var etlDone = WaitForEtl(srcStore, (_, s) => s.LoadSuccesses > 0);

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

                etlDone = WaitForEtl(srcStore, (_, s) => s.LoadSuccesses > 0);
                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = destStore.OpenSession())
                {
                    var contract = session.Load<Contract>("contracts/1-A/ContractsTemp/0000000000000000001-A");

                    Assert.NotNull(contract);
                    Assert.Equal(13, contract.Contact.AdditionalInfo);
                }
            }
        }

        [Fact]
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
                AddEtl(srcStore, destStore, new[] { "Contracts" }, script, out var config);

                using (var session = srcStore.OpenSession())
                {
                    session.Store(new Contract { Contact = new Contact { AdditionalInfo = 10 } });
                    session.SaveChanges();
                }

                EtlErrorInfo error = null;
                WaitForValue(() => TryGetTransformationError(srcStore.Database, config, out error), true, timeout: TimeSpan.FromSeconds(15).Milliseconds);

                Assert.NotNull(error);
                Assert.True(error.Error.Contains($"{nameof(JavaScriptParseException)}: Failed to parse:"));
                Assert.True(error.Error.Contains("Unexpected token ."));
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
