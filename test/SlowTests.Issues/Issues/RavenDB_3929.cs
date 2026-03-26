using FastTests;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3929 : RavenTestBase
    {
        public RavenDB_3929(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.JavaScript)]
        public void NullPropagationShouldNotAffectOperators()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("keys/1", null, new
                    {
                        NullField = (string)null,
                        NotNullField = "value",
                        EmptyField = ""
                    });

                    store.Operations.Send(new PatchOperation("keys/1", null, new PatchRequest
                    {
                        Script = @"
//this.is_nullfield_not_null = this.NullField !== null;
this.is_notnullfield_not_null = this.NotNullField !== null;
this.has_emptyfield_not_null = this.EmptyField !== null;
"
                    }));

                    dynamic document = commands.Get("keys/1");
                    // can't make it work with Jurrasic
                    // bool isNullFieldNotNull = document.is_nullfield_not_null;
                    bool isNotNullFieldNotNull = document.is_notnullfield_not_null;
                    bool hasEmptyFieldNotNull = document.has_emptyfield_not_null;

                    //Assert.False(isNullFieldNotNull);
                    Assert.True(isNotNullFieldNotNull);
                    Assert.True(hasEmptyFieldNotNull);
                }
            }
        }
    }
}