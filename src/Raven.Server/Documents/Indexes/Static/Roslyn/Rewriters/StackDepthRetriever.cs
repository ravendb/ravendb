using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;


namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

public sealed class StackDepthRetriever : CSharpSyntaxRewriter
{
    private static readonly HashSet<string> ChainableLinqMethods = new()
    {
            "Concat", "Select", "SelectMany", "Where", "Skip", "Take",
            "SkipWhile", "TakeWhile", "OrderBy", "OrderByDescending",
            "ThenBy", "ThenByDescending", "Reverse", "Distinct",
            "Union", "Intersect", "Except", "Zip", "DefaultIfEmpty"
        };

    private int _letCounter;
    private int _selectDepth;
    private int _linqChainDepth;

    public int LinqChainDepth => _linqChainDepth;

    public int StackSizeLetCounter => _letCounter + _selectDepth;

    public void Clear()
    {
        _letCounter = 0;
        _selectDepth = 0;
        _linqChainDepth = 0;
    }

    public void VisitMethodQuery(string cSharpCode)
    {
        string origin = string.Empty;
        for (int stackDepth = 0; stackDepth < 100; ++stackDepth)
        {
            var temp = $"this{stackDepth}." + origin;
            if (cSharpCode.Contains(temp))
            {
                origin = temp;
                _selectDepth++;
            }
            else
            {
                break;
            }
        }
    }

    public override SyntaxNode VisitLetClause(LetClauseSyntax queryLetClause)
    {
        _letCounter++;
        return base.VisitLetClause(queryLetClause);
    }

    public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
    {
        if (node.Expression is MemberAccessExpressionSyntax memberAccess &&
            ChainableLinqMethods.Contains(memberAccess.Name.Identifier.Text))
        {
            int depth = CountChainDepth(node);
            if (depth > _linqChainDepth)
                _linqChainDepth = depth;
        }

        return base.VisitInvocationExpression(node);
    }

    private static int CountChainDepth(InvocationExpressionSyntax node)
    {
        int depth = 0;
        SyntaxNode current = node;

        while (current is InvocationExpressionSyntax invocation &&
               invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
               ChainableLinqMethods.Contains(memberAccess.Name.Identifier.Text))
        {
            depth++;
            current = memberAccess.Expression;

            // unwrap further invocations on the left side
            if (current is InvocationExpressionSyntax)
                continue;

            break;
        }

        return depth;
    }
}
