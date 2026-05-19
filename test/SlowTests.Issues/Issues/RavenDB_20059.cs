using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Documents.ETL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_20059 : RavenTestBase
    {
        public RavenDB_20059(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanModifyGlobalObjectUsingEtlTransformScriptWithDeleteDocumentsBehaviorFunction(Options options)
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
                await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

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
                await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                using (var session = destStore.OpenSession())
                {
                    var contract = session.Load<Contract>("contracts/1-A/ContractsTemp/0000000000000000001-A");

                    Assert.NotNull(contract);
                    Assert.Equal(13, contract.Contact.AdditionalInfo);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task WeStillShouldGetErrorWhenEtlProcessRunsBehaviorFunctionWithInvalidSyntax()
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
                Etl.AddEtl(srcStore, destStore, new[] { "Contracts" }, script, out _);

                var etlDone = Etl.WaitForEtlToComplete(srcStore);
                
                using (var session = srcStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new Contract { Contact = new Contact { AdditionalInfo = 10 } });
                    await session.SaveChangesAsync();
                }
                
                await etlDone.WaitAsync(TimeSpan.FromSeconds(5));
                
                var database = await GetDatabase(srcStore.Database);
                
                var processErrors = database.TaskErrorsStorage.ReadAllProcessErrors(TaskCategory.Etl);
                    
                Assert.Single(processErrors);
                Assert.Equal((int)TaskErrorStep.Configuration, processErrors.Single().Step);
                Assert.True(processErrors.Single().Error.Contains($"{nameof(JavaScriptParseException)}: Failed to parse:"));
                Assert.True(processErrors.Single().Error.Contains("Unexpected token '.'"));
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
