using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using System;
using System.IO;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ParserTests
    {
        [Theory]
        [InlineData("Name")]
        [InlineData("Address.City")]
        [InlineData("Address.City.Zone")]
        [InlineData("Tags[]")]
        [InlineData("Tags[].'Basic Units'")]
        [InlineData("'Basic Units'.Tricolor ")]
        public void CanParseField(string q)
        {
            var parser = new QueryParser();
            parser.Init(q);

            FieldToken token;
            Assert.True(parser.Field(out token));
        }

        [Theory]
        [InlineData(" :name ", 5)]
        [InlineData(":age ", 4)]
        public void CanParseParameter(string q, int len)
        {
            var parser = new QueryParser();
            parser.Init(q);

            Assert.True(parser.Parameter(out var start, out var length));
            Assert.Equal(len, length);
        }

        [Theory]
        [InlineData("Name = 'Oren'", OperatorType.Equal)]
        [InlineData("Name < 'Oren'", OperatorType.LessThen)]
        [InlineData("Name <= 'Oren'", OperatorType.LessThenEqual)]
        [InlineData("Name > 'Oren'", OperatorType.GreaterThen)]
        [InlineData("Name >= 'Oren'", OperatorType.GreaterThenEqual)]
        [InlineData("Name between 'Oren' AND 'Phoebe'", OperatorType.Between)]
        [InlineData("(Name between 'Oren' AND 'Phoebe')", OperatorType.Between)]
        [InlineData("Name IN ()", OperatorType.In)]
        [InlineData("(Name IN ())", OperatorType.In)]
        [InlineData("Age in (23)", OperatorType.In)]
        [InlineData("Age in (23,48)", OperatorType.In)]
        [InlineData("Status in ('Active', 'Passive')", OperatorType.In)]
        [InlineData("(Status in ('Active', 'Passive'))", OperatorType.In)]
        [InlineData("State = 2 AND Act = 'Wait'", OperatorType.And)]
        [InlineData("(State = 2 OR Act = 'Wait')", OperatorType.Or)]
        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", OperatorType.OrNot)]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User ='Root')", OperatorType.Or)]
        [InlineData("boost()", OperatorType.Method)]
        [InlineData("boost(User = 'Admin')", OperatorType.Method)]
        [InlineData("boost(User = 'Admin' OR User = 'Root', 2)", OperatorType.Method)]
        [InlineData("boost((User = 'Admin' OR User = 'Root') AND NOT State = 'Active', 2)", OperatorType.Method)]
        public void CanParse(string q, OperatorType type)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            Assert.Equal(type, op.Type);
        }
        
        [Theory]
        [InlineData("Name =     'Oren'", "Name = 'Oren'")]
        [InlineData("Name between \n'Oren' AND 'Phoebe'", "Name BETWEEN 'Oren' AND 'Phoebe'")]
        [InlineData("( Name between 'Oren' AND 'Phoebe' )", "Name BETWEEN 'Oren' AND 'Phoebe'")]
        [InlineData("Name IN ()", "Name IN ()")]
        [InlineData("(Name IN ())", "Name IN ()")]
        [InlineData("Age in (23,48)", "Age IN (23, 48)")]
        [InlineData("(Status in ('Active', 'Passive'))", "Status IN ('Active', 'Passive')")]
        [InlineData("State = 2 AND Act = 'Wait'", "(State = 2 AND Act = 'Wait')")]
        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", "((State = 2 OR Act = 'Wait') OR NOT User = 'Admin')")]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User = 'Root')", "((State = 2 AND NOT User = 'Admin') OR User = 'Root')")]
        [InlineData("boost()", "boost()")]
        [InlineData("boost( User = 'Admin' )", "boost(User = 'Admin')")]
        [InlineData("boost(User = 'Admin' OR User = 'Root', 2)", "boost((User = 'Admin' OR User = 'Root'), 2)")]
        public void ParseAndWrite(string q, string o)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            var output = new StringWriter();
            op.ToString(q, output);
            Assert.Equal(o, output.GetStringBuilder().ToString());
        }
        
        [Theory]
        [InlineData("Name =     'Oren'", "{\"Type\":\"Equal\",\"Field\":\"Name\",\"Value\":\"Oren'\"}")]
        [InlineData("Name between \n'Oren' AND 'Phoebe'", "{\"Type\":\"Between\",\"Field\":\"Name\",\"Min\":\"Oren'\",\"Max\":\"Phoebe'\"}")]
        [InlineData("( Name between 'Oren' AND 'Phoebe' )", "{\"Type\":\"Between\",\"Field\":\"Name\",\"Min\":\"Oren'\",\"Max\":\"Phoebe'\"}")]
        [InlineData("Name IN ()", "{\"Type\":\"In\",\"Field\":\"Name\",\"Values\":[]}")]
        [InlineData("(Name IN ())", "{\"Type\":\"In\",\"Field\":\"Name\",\"Values\":[]}")]
        [InlineData("Age in (23,48)", "{\"Type\":\"In\",\"Field\":\"Age\",\"Values\":[23,48]}")]
        [InlineData("(Status in ('Active', 'Passive'))", "{\"Type\":\"In\",\"Field\":\"Status\",\"Values\":[\"Active'\",\"Passive'\"]}")]
        [InlineData("State = 2 AND Act = 'Wait'", "{\"Type\":\"And\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"State\",\"Value\":\"2\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"Act\",\"Value\":\"Wait'\"}}")]
        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", "{\"Type\":\"OrNot\",\"Left\":{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"State\",\"Value\":\"2\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"Act\",\"Value\":\"Wait'\"}},\"Right\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin'\"}}")]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User = 'Root')", "{\"Type\":\"Or\",\"Left\":{\"Type\":\"AndNot\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"State\",\"Value\":\"2\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin'\"}},\"Right\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Root'\"}}")]
        [InlineData("boost()", "{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[]}")]
        [InlineData("boost( User = 'Admin' )", "{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin'\"}]}")]
        [InlineData("boost(User = 'Admin' OR User = 'Root', 2)", "{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin'\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Root'\"}},2]}")]
        public void ParseAndWriteAst(string q, string o)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            var output = new StringWriter();
            op.ToJsonAst(q, new JsonTextWriter(output));
            Assert.Equal(o, output.GetStringBuilder().ToString());
        }
    }
}