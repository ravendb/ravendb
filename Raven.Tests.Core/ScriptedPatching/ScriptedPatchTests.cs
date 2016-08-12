using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Core.ScriptedPatching
{
    public class ScriptedPatchTests : RavenCoreTestBase
    {
        public class Foo
        {
            public string Id { get; set; }

            public string BarId { get; set; }

            public string FirstName { get; set; }

            public string Fullname { get; set; }
        }

        public class Bar
        {
            public string Id { get; set; }

            public string LastName { get; set; }
        }

        [Fact]
        public void Max_script_steps_can_be_increased_from_inside_script()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document =>
            {
                document.Settings["Raven/MaxStepsForScript"] = "5000";
                document.Settings[Constants.AllowScriptsToAdjustNumberOfSteps] = "true";
            }))
            {
                var foo = new Foo
                {
                    BarId = "bar/1",
                    FirstName = "Joe"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Bar
                    {
                        Id = "bar/1",
                        LastName = "Doe"
                    });

                    session.Store(foo);
                    session.SaveChanges();
                }

                Assert.Throws<ErrorResponseException>(() =>
                {
                    store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                    {
                        Script = @"for(var i = 0;i < 7500;i++){}"
                    });
                });

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"IncreaseNumberOfAllowedStepsBy(4500); for(var i = 0;i < 7500;i++){}"
                });
            }
        }

        [Fact]
        public void Load_document_should_increase_max_steps_in_algorithm()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document =>
            {
                document.Settings["Raven/MaxStepsForScript"] = "5000";
                document.Settings[Constants.AllowScriptsToAdjustNumberOfSteps] = "true";
            }))
            {
                var foo = new Foo
                {
                    BarId = "bar/1",
                    FirstName = "Joe"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Bar
                    {
                        Id = "bar/1",
                        LastName = "Doe"
                    });

                    session.Store(foo);
                    session.SaveChanges();
                }

                Assert.Throws<ErrorResponseException>(() =>
                {
                    store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                    {
                        Script = @"for(var i = 0;i < 7500;i++){}"
                    });
                });

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"LoadDocument('bar/1'); for(var i = 0;i < 7500;i++){}"
                });
            }
        }
    }
}
