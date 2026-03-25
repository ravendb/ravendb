using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Documents.Commands;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24072 : RavenTestBase
{
    public RavenDB_24072(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Vector)]
    public async Task CanGenerateClassWithRavenVector()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var dto = new Dto()
                {
                    Id = "dtos/1",
                    Name = "TestName", 
                    FloatsVector = new RavenVector<float>([0.1f, 0.2f, 0.3f]), 
                    BytesVector = new RavenVector<byte>([21, 37]), 
                    SbytesVector = new RavenVector<sbyte>([21, 37]),
                    SubDto = new SubDto()
                    {
                        Subvector = new RavenVector<float>([0.5f, 0.6f])
                    }
                };
                
                await session.StoreAsync(dto);
                await session.SaveChangesAsync();

                var command = new GenerateClassFromDocumentCommand("dtos/1", collection: null, "csharp");

                await session.Advanced.RequestExecutor.ExecuteAsync(command, session.Advanced.Context);

                var @class = command.Result;

                RavenTestHelper.AssertEqualRespectingNewLines(ExpectedResult, @class);
            }
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public RavenVector<float> FloatsVector { get; set; }
        public RavenVector<byte> BytesVector { get; set; }
        public RavenVector<sbyte> SbytesVector { get; set; }
        public SubDto SubDto { get; set; }
    }

    private class SubDto
    {
        public RavenVector<float> Subvector { get; set; }
    }
    
    private const string ExpectedResult = """
                                          using System;
                                          using System.Collections.Generic;
                                          using System.Linq;
                                          using System.Text;
                                          using System.Threading.Tasks;

                                          namespace SlowTests.Issues
                                          {
                                              public class SubDto
                                              {
                                                  public RavenVector<float> Subvector { get; set; } 
                                              }
                                          
                                              public class RavenDB_24072+Dto
                                              {
                                                  public RavenVector<sbyte> BytesVector { get; set; } 
                                                  public RavenVector<float> FloatsVector { get; set; } 
                                                  public string Name { get; set; } 
                                                  public RavenVector<sbyte> SbytesVector { get; set; } 
                                                  public SubDto SubDto { get; set; } 
                                              }
                                          }
                                          """;
}
