using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class RavenLinqPrettifier : CSharpSyntaxRewriter
    {
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var memeberAccess = node.Expression as MemberAccessExpressionSyntax;
            if (memeberAccess == null || node.ArgumentList.Arguments.Count < 1 || !AllParentsAreMethods(node))
                return base.VisitInvocationExpression(node);

            switch (memeberAccess.Name.Identifier.ValueText)
            {
                case "SelectMany":
                    var sourceExp = GetSimpleLambdaExpressionSyntax(node);
                    var selectorExp = GetSelectorLambdaFromSelectManyExpression(node);

                    if (sourceExp == null || selectorExp == null || selectorExp.ParameterList.Parameters.Count < 2)
                        return base.VisitInvocationExpression(node);

                    sourceExp = sourceExp.WithParameter(selectorExp.ParameterList.Parameters[0])
                        .WithBody(SyntaxNodeExtensions.ReplaceNodes<CSharpSyntaxNode, IdentifierNameSyntax>(sourceExp.Body, Enumerable.OfType<IdentifierNameSyntax>(sourceExp.Body.DescendantNodes()), (orig, _) =>
                        {
                            if (orig.Parent is MemberAccessExpressionSyntax access && orig == access.Name)
                                return orig;

                            if (orig.Identifier.ValueText != sourceExp.Parameter.Identifier.ValueText)
                                return orig;
                            return orig.WithIdentifier(selectorExp.ParameterList.Parameters[0].Identifier);
                        }));

                    var sourceExpBody = GetBodyAndRemoveCastingIfNeeded(sourceExp);
                    var selectManyInvocExp = memeberAccess.Expression as InvocationExpressionSyntax;
                    MemberAccessExpressionSyntax selectManyInnerMemberAccess = null;
                    if (selectManyInvocExp != null)
                        selectManyInnerMemberAccess = selectManyInvocExp.Expression as MemberAccessExpressionSyntax;

                    if (selectManyInnerMemberAccess == null)
                    {
                        //handle innermost SelectMany()
                        return SyntaxFactory.QueryExpression(
                            SyntaxFactory.FromClause(
                                SyntaxFactory.Identifier(sourceExp.Parameter.Identifier.ValueText),
                                (ExpressionSyntax)Visit(memeberAccess.Expression)),
                            SyntaxFactory.QueryBody(
                                    SyntaxFactory.SelectClause((ExpressionSyntax)selectorExp.Body))
                                .WithClauses(
                                    SyntaxFactory.SingletonList<QueryClauseSyntax>(
                                        SyntaxFactory.FromClause(
                                            (SyntaxToken)selectorExp.ParameterList.Parameters[1].Identifier,
                                            (ExpressionSyntax)sourceExpBody
                                        )
                                    )));
                    }

                    if (selectManyInnerMemberAccess.Name.Identifier.ValueText != "SelectMany")
                        return base.VisitInvocationExpression(node);

                    //handle docs.SelectMany().SelectMany()...
                    var innerQueryExp = (QueryExpressionSyntax)Visit(selectManyInvocExp);

                    var clausesList = innerQueryExp.Body.Clauses.Add(SyntaxFactory.LetClause(
                        SyntaxFactory.Identifier(selectorExp.ParameterList.Parameters[0].Identifier.ValueText),
                        (ExpressionSyntax)Visit((innerQueryExp.Body.SelectOrGroup as SelectClauseSyntax)?.Expression)));
                    clausesList = clausesList.Add(SyntaxFactory.FromClause(
                        SyntaxFactory.Identifier(selectorExp.ParameterList.Parameters[1].Identifier.ValueText),
                        (ExpressionSyntax)Visit(sourceExpBody)));

                    //TODO : aviv - try to remove 'this0', 'this1', etc.. from sourceExp and selectorExp,
                    //TODO          so that we can avoid adding 'let' clauses to QueryBody 

                    return SyntaxFactory.QueryExpression(
                        innerQueryExp.FromClause,
                        SyntaxFactory.QueryBody(
                                SyntaxFactory.SelectClause((ExpressionSyntax)Visit(selectorExp.Body)))
                            .WithClauses(clausesList));


                case "Select":
                    var expressionSyntax = GetSimpleLambdaExpressionSyntax(node);
                    if (expressionSyntax == null)
                        return base.VisitInvocationExpression(node);

                    var invocExp = memeberAccess.Expression as InvocationExpressionSyntax;
                    MemberAccessExpressionSyntax innerMemberAccess = null;
                    if (invocExp != null)
                        innerMemberAccess = invocExp.Expression as MemberAccessExpressionSyntax;

                    var name = innerMemberAccess?.Name.Identifier.ValueText;

                    var innerInvocExp = innerMemberAccess?.Expression as InvocationExpressionSyntax;
                    MemberAccessExpressionSyntax innerInnerMemberAccess = null;
                    if (innerInvocExp != null)
                        innerInnerMemberAccess = innerInvocExp.Expression as MemberAccessExpressionSyntax;

                    if (innerMemberAccess == null || name == "Select" && innerInnerMemberAccess == null)
                        //handle docs.Select()
                        return SyntaxFactory.QueryExpression(
                            SyntaxFactory.FromClause(
                                (SyntaxToken)expressionSyntax.Parameter.Identifier,
                                RavenLinqOptimizer.MaybeParenthesizedExpression((ExpressionSyntax)Visit(memeberAccess.Expression))
                            ),
                            SyntaxFactory.QueryBody(SyntaxFactory.SelectClause((ExpressionSyntax)Visit(expressionSyntax.Body))));

                    if (name != "Where")
                        return base.VisitInvocationExpression(node);

                    var whereClause = GetSimpleLambdaExpressionSyntax(invocExp);

                    if (innerInnerMemberAccess != null && innerInnerMemberAccess.Name.Identifier.ValueText == "SelectMany")
                    {
                        //handle docs.SelectMany().Where().Select()
                        var innerQueryExpSyntax = (QueryExpressionSyntax)Visit(innerInvocExp);
                        var clauses = innerQueryExpSyntax.Body.Clauses.Add(SyntaxFactory.LetClause(
                            SyntaxFactory.Identifier(whereClause.Parameter.Identifier.ValueText),
                            (ExpressionSyntax)Visit((innerQueryExpSyntax.Body.SelectOrGroup as SelectClauseSyntax)?.Expression)));
                        clauses = clauses.Add(SyntaxFactory.WhereClause((ExpressionSyntax)Visit(whereClause.Body)));

                        return SyntaxFactory.QueryExpression(
                            innerQueryExpSyntax.FromClause,
                            SyntaxFactory.QueryBody(
                                    SyntaxFactory.SelectClause((ExpressionSyntax)Visit(expressionSyntax.Body)))
                                .WithClauses(clauses));
                    }

                    //handle docs.Where().Select()
                    var identifierNameSyntax = innerMemberAccess.Expression as IdentifierNameSyntax;
                    if (identifierNameSyntax == null)
                        return base.VisitInvocationExpression(node);

                    whereClause = whereClause.WithParameter(expressionSyntax.Parameter)
                        .WithBody(SyntaxNodeExtensions.ReplaceNodes<CSharpSyntaxNode, IdentifierNameSyntax>(whereClause.Body, Enumerable.OfType<IdentifierNameSyntax>(whereClause.Body.DescendantNodes()), (orig, _) =>
                        {
                            if (orig.Parent is MemberAccessExpressionSyntax access && orig == access.Name)
                                return orig;

                            if (orig.Identifier.ValueText != whereClause.Parameter.Identifier.ValueText)
                                return orig;
                            return orig.WithIdentifier(expressionSyntax.Parameter.Identifier);
                        }));

                    return SyntaxFactory.QueryExpression(
                        SyntaxFactory.FromClause(
                            (SyntaxToken)expressionSyntax.Parameter.Identifier,
                            (ExpressionSyntax)SyntaxFactory.IdentifierName(identifierNameSyntax.Identifier.ValueText)
                        ),
                        SyntaxFactory.QueryBody(SyntaxFactory.SelectClause((ExpressionSyntax)Visit(expressionSyntax.Body)))
                            .WithClauses(SyntaxFactory.SingletonList<QueryClauseSyntax>(SyntaxFactory.WhereClause((ExpressionSyntax)Visit(whereClause.Body)))));

                default:
                    return base.VisitInvocationExpression(node);

            }

        }

        private static CSharpSyntaxNode GetBodyAndRemoveCastingIfNeeded(SimpleLambdaExpressionSyntax sourceExp)
        {
            var sourceExpBody = sourceExp.Body;
            var castExpressionSyntax = sourceExpBody as CastExpressionSyntax;

            if (castExpressionSyntax != null)
                sourceExpBody = RavenLinqOptimizer.StripExpressionParenthesis(castExpressionSyntax.Expression);
            return sourceExpBody;
        }

        private static ParenthesizedLambdaExpressionSyntax GetSelectorLambdaFromSelectManyExpression(InvocationExpressionSyntax node)
        {
            if (node.ArgumentList.Arguments.Count < 2)
                return null;

            ParenthesizedLambdaExpressionSyntax expressionSyntax;
            var castExpressionSyntax = node.ArgumentList.Arguments[1].Expression as CastExpressionSyntax;

            if (castExpressionSyntax != null)
                expressionSyntax = RavenLinqOptimizer.StripExpressionParenthesis(castExpressionSyntax.Expression) as ParenthesizedLambdaExpressionSyntax;
            else
                expressionSyntax = node.ArgumentList.Arguments[1].Expression as ParenthesizedLambdaExpressionSyntax;
            return expressionSyntax;
        }

        private static SimpleLambdaExpressionSyntax GetSimpleLambdaExpressionSyntax(InvocationExpressionSyntax node)
        {
            SimpleLambdaExpressionSyntax expressionSyntax;
            var castExpressionSyntax = node.ArgumentList.Arguments[0].Expression as CastExpressionSyntax;

            if (castExpressionSyntax != null)
                expressionSyntax = RavenLinqOptimizer.StripExpressionParenthesis(castExpressionSyntax.Expression) as SimpleLambdaExpressionSyntax;
            else
                expressionSyntax = node.ArgumentList.Arguments[0].Expression as SimpleLambdaExpressionSyntax;
            return expressionSyntax;
        }

        private static bool AllParentsAreMethods(InvocationExpressionSyntax node)
        {
            var es = node.Parent;

            while (es != null)
            {
                if (!(es is InvocationExpressionSyntax))
                {
                    // handle docs.Books.Select( x=> ... ); 
                    if (es is MemberAccessExpressionSyntax mae && mae.Parent is InvocationExpressionSyntax)
                    {
                        es = mae.Parent;
                        continue;
                    }
                    return false;
                }
                es = es.Parent;
            }

            return true;
        }
    }
}