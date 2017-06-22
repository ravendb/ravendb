using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    public class CaptureSelectNewFieldNamesVisitor : CSharpSyntaxVisitor
    {
        private readonly bool _outerMostRequired;
        private readonly HashSet<string> _fieldNames;
        private readonly Dictionary<string, ExpressionSyntax> _selectExpressions;
        private bool _queryProcessed;

        public CaptureSelectNewFieldNamesVisitor(bool outerMostRequired, HashSet<string> fieldNames, Dictionary<string, ExpressionSyntax> selectExpressions)
        {
            _outerMostRequired = outerMostRequired;
            _fieldNames = fieldNames;
            _selectExpressions = selectExpressions;
        }

        public static SyntaxNode GetAnonymousCreateExpression(SyntaxNode expression)
        {
            var invocationExpression = expression as InvocationExpressionSyntax;

            if (invocationExpression == null)
                return expression;
            var member = invocationExpression.Expression as MemberAccessExpressionSyntax;
            if (member == null)
                return expression;

            var typeReference = member.Expression as RefTypeExpressionSyntax;
            if (typeReference == null)
            {
                var objectCreateExpression = member.Expression as AnonymousObjectCreationExpressionSyntax;
                if (objectCreateExpression != null && member.Name.ToString() == "Boost")
                {
                    return objectCreateExpression;
                }
                return expression;
            }
            switch (member.Name.ToString())
            {
                case "Boost":
                    return invocationExpression.ArgumentList.Arguments.First();
            }
            return expression;
        }

        public void Clear()
        {
            _queryProcessed = false;
            _fieldNames.Clear();
            _selectExpressions.Clear();
        }

        public void ProcessQuery(SyntaxNode queryExpressionSelectClause)
        {
            var objectCreateExpression = GetAnonymousCreateExpression(queryExpressionSelectClause) as AnonymousObjectCreationExpressionSyntax;
            if (objectCreateExpression == null)
                return;

            // we only want the outer most value
            if (_queryProcessed && _outerMostRequired)
                return;

            _fieldNames.Clear();
            _selectExpressions.Clear();

            _queryProcessed = true;

            foreach (var initializer in objectCreateExpression.Initializers)
            {
                CollecetFieldNamesAndSelectsFromMemberDeclarator(initializer);
            }
        }

        private void CollecetFieldNamesAndSelectsFromMemberDeclarator(AnonymousObjectMemberDeclaratorSyntax expression)
        {
            string name;

            if (expression.NameEquals != null)
            {
                name = expression.NameEquals.Name.Identifier.ValueText;
            }

            else if(expression.Expression is MemberAccessExpressionSyntax memberAccess)
            { 
                name = memberAccess.Name.Identifier.ValueText;
            }

            else return;

            _fieldNames.Add(name);
            _selectExpressions[name] = expression.Expression;
        }

        public override void VisitInvocationExpression(InvocationExpressionSyntax invocationExpression)
        {
            var memberReferenceExpression = invocationExpression.Expression as MemberAccessExpressionSyntax;

            if (memberReferenceExpression == null)
            {
                base.VisitInvocationExpression(invocationExpression);
            }

            LambdaExpressionSyntax lambdaExpression = null;
            switch (memberReferenceExpression?.Name.ToString())
            {
                case "Select":
                    if (invocationExpression.ArgumentList.Arguments.Count != 1)
                        base.VisitInvocationExpression(invocationExpression);
                    lambdaExpression = invocationExpression.ArgumentList.Arguments.First().Expression as LambdaExpressionSyntax;
                    break;
                case "SelectMany":
                    if (invocationExpression.ArgumentList.Arguments.Count != 2)
                        base.VisitInvocationExpression(invocationExpression);
                    lambdaExpression = invocationExpression.ArgumentList.Arguments.ElementAt(1).Expression as LambdaExpressionSyntax;
                    break;
                default:
                    base.VisitInvocationExpression(invocationExpression);
                    break;
            }

            if (lambdaExpression == null)
            {
                base.VisitInvocationExpression(invocationExpression);
            }

            ProcessQuery(lambdaExpression?.Body);

            base.VisitInvocationExpression(invocationExpression);
        }

        public override void VisitSelectClause(SelectClauseSyntax selectClause)
        {
            ProcessQuery(selectClause.Expression);
            if (_outerMostRequired)
                base.VisitSelectClause(selectClause);
        }
    }

}
