using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5051 : RavenTestBase
    {
        public RavenDB_5051(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void FormatShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var result = store.Maintenance.Send(new FormatOperation(@"from c in docs.Companies
                select new
                {
                    Name = c.Name
                }"));

                //\r\n line terminators are expected regardless of the OS type.
                //The underlying SyntaxFactory and Formatter classes and will produce \r\n.
                var expected = PlatformDetails.RunningOnPosix
                    ? "from c in docs.Companies\r\nselect new\r\n{\n    Name = c.Name\r\n}"
                    : "from c in docs.Companies\r\nselect new\r\n{\r\n    Name = c.Name\r\n}";

                Assert.Equal(expected, result.Expression);
            }
        }

        private class FormatOperation : IMaintenanceOperation<FormatOperation.Result>
        {
            private readonly string _expression;

            public class Result
            {
                public string Expression { get; set; }
            }

            public FormatOperation(string expression)
            {
                _expression = expression;
            }

            public RavenCommand<Result> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new FormatCommand(context, _expression);
            }

            private class FormatCommand : RavenCommand<Result>
            {
                private readonly JsonOperationContext _context;
                private readonly string _expression;

                public FormatCommand(JsonOperationContext context, string expression)
                {
                    _context = context;
                    _expression = expression;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/studio-tasks/format";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        Content = new BlittableJsonContent(async stream =>
                        {
                            await using (var writer = new AsyncBlittableJsonTextWriter(_context, stream))
                            {
                                writer.WriteStartObject();

                                writer.WritePropertyName(nameof(FormatOperation.Result.Expression));
                                writer.WriteString(_expression);

                                writer.WriteEndObject();
                            }
                        })
                    };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    response.TryGet(nameof(FormatOperation.Result.Expression), out string function);

                    Result = new Result
                    {
                        Expression = function
                    };
                }
            }
        }
    }
}
