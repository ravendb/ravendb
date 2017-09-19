using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using System;
using System.IO;
using Raven.Server.Documents.Queries.AST;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ParserTests : NoDisposalNeeded
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

            FieldExpression token;
            Assert.True(parser.Field(out token));
        }

        [Theory]
        [InlineData(" $name ", 4)]
        [InlineData("$age ", 3)]
        public void CanParseParameter(string q, int len)
        {
            var parser = new QueryParser();
            parser.Init(q);

            Assert.True(parser.Parameter(out var start, out var length));
            Assert.Equal(len, length);
        }

        [Theory]
        [InlineData("Name = 'Oren'", OperatorType.Equal)]
        [InlineData("Name < 'Oren'", OperatorType.LessThan)]
        [InlineData("Name <= 'Oren'", OperatorType.LessThanEqual)]
        [InlineData("Name > 'Oren'", OperatorType.GreaterThan)]
        [InlineData("Name >= 'Oren'", OperatorType.GreaterThanEqual)]
     
        [InlineData("State = 2 AND Act = 'Wait'", OperatorType.And)]
        [InlineData("(State = 2 OR Act = 'Wait')", OperatorType.Or)]
        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", OperatorType.OrNot)]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User ='Root')", OperatorType.AndNot)]
       public void CanParse(string q, OperatorType type)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            Assert.IsType<BinaryExpression>(op);
            Assert.Equal(type, ((BinaryExpression)op).Operator);
        }
        
        [InlineData("Name between 'Oren' AND 'Phoebe'", typeof(BetweenExpression))]
        [InlineData("(Name between 'Oren' AND 'Phoebe')", typeof(BetweenExpression))]
        [InlineData("boost()", typeof(MethodExpression))]
        [InlineData("boost(User = 'Admin')", typeof(MethodExpression))]
        [InlineData("boost(User = 'Admin' OR User = 'Root', 2)", typeof(MethodExpression))]
        [InlineData("boost((User = 'Admin' OR User = 'Root') AND NOT State = 'Active', 2)", typeof(MethodExpression))]
        [InlineData("Name IN ()", typeof(InExpression))]
        [InlineData("(Name IN ())", typeof(InExpression))]
        [InlineData("Age in (23)", typeof(InExpression))]
        [InlineData("Age in (23,48)",typeof(InExpression))]
        [InlineData("Status in ('Active', 'Passive')", typeof(InExpression))]
        [InlineData("(Status in ('Active', 'Passive'))",typeof(InExpression))]
        public void CanParse2(string q, Type type)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            Assert.Equal(type, op.GetType());
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
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User = 'Root')", "(State = 2 AND NOT (User = 'Admin' OR User = 'Root'))")]
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
            new StringQueryVisitor(output.GetStringBuilder()).VisitExpression(op);
            Assert.Equal(o, output.GetStringBuilder().ToString());
        }
        
        [Theory]
        [InlineData("Name =     'Oren'", "{\"Type\":\"Equal\",\"Field\":\"Name\",\"Value\":\"Oren\"}")]
        [InlineData("Name between \n'Oren' AND 'Phoebe'", "{\"Type\":\"Between\",\"Field\":\"Name\",\"Min\":\"Oren\",\"Max\":\"Phoebe\"}")]
        [InlineData("( Name between 'Oren' AND 'Phoebe' )", "{\"Type\":\"Between\",\"Field\":\"Name\",\"Min\":\"Oren\",\"Max\":\"Phoebe\"}")]
        [InlineData("Name IN ()", "{\"Type\":\"In\",\"Field\":\"Name\",\"Values\":[]}")]
        [InlineData("(Name IN ())", "{\"Type\":\"In\",\"Field\":\"Name\",\"Values\":[]}")]
        [InlineData("Age in (23,48)", "{\"Type\":\"In\",\"Field\":\"Age\",\"Values\":[23,48]}")]
        [InlineData("(Status in ('Active', 'Passive'))", "{\"Type\":\"In\",\"Field\":\"Status\",\"Values\":[\"Active\",\"Passive\"]}")]
        [InlineData("State = 2 AND Act = 'Wait'", "{\"Type\":\"And\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"State\",\"Value\":\"2\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"Act\",\"Value\":\"Wait\"}}")]
        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", "{\"Type\":\"OrNot\",\"Left\":{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"State\",\"Value\":\"2\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"Act\",\"Value\":\"Wait\"}},\"Right\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin\"}}")]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User = 'Root')", "{\"Type\":\"AndNot\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"State\",\"Value\":\"2\"},\"Right\":{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Root\"}}}")]
        [InlineData("boost()", "{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[]}")]
        [InlineData("boost( User = 'Admin' )", "{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin\"}]}")]
        [InlineData("boost(User = 'Admin' OR User = 'Root', 2)", "{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Admin\"},\"Right\":{\"Type\":\"Equal\",\"Field\":\"User\",\"Value\":\"Root\"}},2]}")]
        public void ParseAndWriteAst(string q, string o)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            var output = new StringWriter();
            new JsonQueryVisitor(new JsonTextWriter(output)).VisitExpression(op);
            Assert.Equal(o, output.GetStringBuilder().ToString());
        }
    }
}
