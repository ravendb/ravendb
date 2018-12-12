using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using System;
using System.IO;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ParserTests : NoDisposalNeeded
    {
        [Theory]
        [InlineData(@"from Orders
select ID('not valid argument')", QueryType.Select)]
        [InlineData(@"declare function Name() {
    var a = 's{tri}}}ng""';
    var b = function () {
        var a = ""'"";
    }
    var c = 's{tri}}}ng';
}


from Orders as o
where o.Company == ''
load o.Company as c, o.ShipTo.  as e, o.ShipVia as s
select {
    Name: Value,
    var a = a;
}", QueryType.Select)]
        [InlineData(@"declare function Name() {
   var a = ;
}


from Orders as o
where o.Company == ''
load o.Company as c, o.ShipTo.  as e, o.ShipVia as s
select {
    Name: Value,
    var a = a;
}", QueryType.Select)]
        [InlineData(@"
from Orders as o
update { 
    this.++;
}
", QueryType.Update)]
        public void FailToParseInvalidJavascript(string q, QueryType type)
        {
            Assert.Throws<InvalidQueryException>(() => new QueryMetadata(q, null, 1, type));
        }

        [Theory]
        [InlineData(@"
declare function Name() {
    var a = ""{{\"""";
        var b = '\'{{'
    }
from Orders as o
    where o.Company == """"
load o.Company as c, o.ShipTo as e, o.ShipVia as s
select {
    Name:
    Value
}", QueryType.Select)]
        [InlineData(@"FROM INDEX 'Orders/Totals' 
WHERE Employee = $emp 
UPDATE {
    for(var i = 0; i < this.Lines.length; i++)
    {
        this.Lines[i].Discount = Math.max(this.Lines[i].Discount || 0, discount);
    }
}", QueryType.Update)]
        public void ParseQueries(string q, QueryType type)
        {
            var parser = new QueryParser();
            parser.Init(q);

            parser.Parse(type);
        }

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

            Assert.True(parser.Parameter(out var token));
            Assert.Equal(len, token.Length);
        }

        [Theory]
        [InlineData("Name = 'Oren'", OperatorType.Equal)]
        [InlineData("Name < 'Oren'", OperatorType.LessThan)]
        [InlineData("Name <= 'Oren'", OperatorType.LessThanEqual)]
        [InlineData("Name > 'Oren'", OperatorType.GreaterThan)]
        [InlineData("Name >= 'Oren'", OperatorType.GreaterThanEqual)]

        [InlineData("State = 2 AND Act = 'Wait'", OperatorType.And)]
        [InlineData("(State = 2 OR Act = 'Wait')", OperatorType.Or)]
        public void CanParse(string q, OperatorType type)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            Assert.IsType<BinaryExpression>(op);
            Assert.Equal(type, ((BinaryExpression)op).Operator);
        }

        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", OperatorType.Or)]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User ='Root')", OperatorType.And)]
        public void CanParseNegated(string q, OperatorType type)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            Assert.IsType<BinaryExpression>(op);
            Assert.Equal(type, ((BinaryExpression)op).Operator);
            Assert.IsType<NegatedExpression>(((BinaryExpression)op).Right);
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
        [InlineData("Age in (23,48)", typeof(InExpression))]
        [InlineData("Status in ('Active', 'Passive')", typeof(InExpression))]
        [InlineData("(Status in ('Active', 'Passive'))", typeof(InExpression))]
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
        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", "((State = 2 OR Act = 'Wait') OR NOT (User = 'Admin'))")]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User = 'Root')", "(State = 2 AND NOT ((User = 'Admin' OR User = 'Root')))")]
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
        [InlineData("State = 2 AND Act = 'Wait'", "{\"Type\":\"And\",\"Left\":{\"Type\":\"Equal\",\"Left\":\"State\",\"Right\":2},\"Right\":{\"Type\":\"Equal\",\"Left\":\"Act\",\"Right\":\"Wait\"}}")]
        [InlineData("( Name between 'Oren' AND 'Phoebe' )", "{\"Between\":{\"Min\":\"Oren\",\"Max\":\"Phoebe\"}}")]
        [InlineData("Name IN ()", "{\"In\":[]}")]
        [InlineData("Age in (23,48)", "{\"In\":[23,48]}")]
        [InlineData("State = 2 AND NOT (User = 'Admin' OR User = 'Root')", "{\"Type\":\"And\",\"Left\":{\"Type\":\"Equal\",\"Left\":\"State\",\"Right\":2},\"Right\":{\"Type\":\"Not\",\"Expression\":{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Left\":\"User\",\"Right\":\"Admin\"},\"Right\":{\"Type\":\"Equal\",\"Left\":\"User\",\"Right\":\"Root\"}}}}")]
        [InlineData("Name =     'Oren'", "{\"Type\":\"Equal\",\"Left\":\"Name\",\"Right\":\"Oren\"}")]
        [InlineData("boost()", "{\"Method\":\"boost\",\"Arguments\":[]}")]
        [InlineData("(State = 2 OR Act = 'Wait') OR NOT User = 'Admin'", "{\"Type\":\"Or\",\"Left\":{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Left\":\"State\",\"Right\":2},\"Right\":{\"Type\":\"Equal\",\"Left\":\"Act\",\"Right\":\"Wait\"}},\"Right\":{\"Type\":\"Not\",\"Expression\":{\"Type\":\"Equal\",\"Left\":\"User\",\"Right\":\"Admin\"}}}")]
        [InlineData("boost(User = 'Admin' OR User = 'Root', 2)", "{\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"Or\",\"Left\":{\"Type\":\"Equal\",\"Left\":\"User\",\"Right\":\"Admin\"},\"Right\":{\"Type\":\"Equal\",\"Left\":\"User\",\"Right\":\"Root\"}},2]}")]
        [InlineData("(Name IN ())", "{\"In\":[]}")]
        [InlineData("(Status in ('Active', 'Passive'))", "{\"In\":[\"Active\",\"Passive\"]}")]
        [InlineData("Name between 'Oren' AND 'Phoebe'", "{\"Between\":{\"Min\":\"Oren\",\"Max\":\"Phoebe\"}}")]
        [InlineData("boost( User = 'Admin' )", "{\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"Equal\",\"Left\":\"User\",\"Right\":\"Admin\"}]}")]
        public void ParseAndWriteAst(string q, string o)
        {
            var parser = new QueryParser();
            parser.Init(q);

            QueryExpression op;
            Assert.True(parser.Expression(out op));
            var output = new StringWriter();
            new JsonQueryVisitor(new JsonTextWriter(output)).VisitExpression(op);
            var actual = output.GetStringBuilder().ToString();
            Assert.Equal(o, actual);
        }
    }
}
