using System;
using System.IO;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.Ast;

namespace Raven.Database.Linq
{
    public class QueryParsingUtils
    {
        public static VariableDeclaration GetVariableDeclaration(string query)
        {
            var parser = ParserFactory.CreateParser(SupportedLanguage.CSharp, new StringReader("var q = " + query));

            var block = parser.ParseBlock();

            if (block.Children.Count != 1)
                throw new InvalidOperationException("Only one statement is allowed");

            var declaration = block.Children[0] as LocalVariableDeclaration;
            if (declaration == null)
                throw new InvalidOperationException("Only local variable decleration are allowed");

            if (declaration.Variables.Count != 1)
                throw new InvalidOperationException("Only one variable declaration is allowed");

            var variable = declaration.Variables[0];

            if (variable.Initializer == null)
                throw new InvalidOperationException("Variable declaration must have an initializer");

            var queryExpression = (variable.Initializer as QueryExpression);
            if (queryExpression == null)
                throw new InvalidOperationException("Variable initializer must be a query expression");

            var selectClause = queryExpression.SelectOrGroupClause as QueryExpressionSelectClause;
            if (selectClause == null)
                throw new InvalidOperationException("Variable initializer must be a select query expression");

            var createExpression = selectClause.Projection as ObjectCreateExpression;
            if (createExpression == null || createExpression.IsAnonymousType == false)
                throw new InvalidOperationException(
                    "Variable initializer must be a select query expression returning an anonymous object");

            return variable;
        }
        
    }
}