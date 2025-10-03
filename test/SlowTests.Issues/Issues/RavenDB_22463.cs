using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Server.Documents.Commands;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22463 : RavenTestBase
{
    public RavenDB_22463(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Can_Extract_Class_Name_From_Collection()
    {
        using (var store = GetDocumentStore())
        {
            using (var commands = store.Commands())
            {
                await commands.PutAsync("cats/1", null, new Cat
                {
                    Name = "Mr Kittenz",
                    Birthday = DateTime.Now,
                    BirthdayOffset = DateTimeOffset.UtcNow
                }, new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "Cats" } });
            }

            using (var session = store.OpenAsyncSession())
            {
                var command = new GenerateClassFromDocumentCommand("cats/1", "csharp");

                await session.Advanced.RequestExecutor.ExecuteAsync(command, session.Advanced.Context);

                var @class = command.Result;

                RavenTestHelper.AssertEqualRespectingNewLines(ExpectedResult, @class);
            }
        }
    }

    private class Cat
    {
        public string Name { get; set; }

        public DateTime Birthday { get; set; }

        public DateTimeOffset BirthdayOffset { get; set; }
    }

    private const string ExpectedResult = """
                                          using System;
                                          using System.Collections.Generic;
                                          using System.Linq;
                                          using System.Text;
                                          using System.Threading.Tasks;
                                          
                                          namespace My.RavenDB
                                          {
                                              public class Cat
                                              {
                                                  public DateTime Birthday { get; set; } 
                                                  public DateTimeOffset BirthdayOffset { get; set; } 
                                                  public string Name { get; set; } 
                                              }
                                          }
                                          """;
}
