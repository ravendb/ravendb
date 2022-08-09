using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;


namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

public class StackDepthRetriever : CSharpSyntaxRewriter
{
    private int _letCounter;

    private int _selectDepth;

    public int StackSize
    {
        get
        {
            //Skip last select {}
            if (_selectDepth > 0)
                _selectDepth--;
            
            return _letCounter + _selectDepth;
        }
    }

    public void Clear()
    {
        _letCounter = 0;
        _selectDepth = 0;
    }

    public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
    {
        var memberAccess = node.Expression as MemberAccessExpressionSyntax;
        if (memberAccess == null || node.ArgumentList.Arguments.Count < 1)
            return base.VisitInvocationExpression(node);

        switch (memberAccess.Name.Identifier.ValueText)
        {
            case "Select":
            case "SelectMany":
                _selectDepth++;
            break;
            default:
                break;
        }

        return base.VisitInvocationExpression(node);
    }

    public override SyntaxNode VisitLetClause(LetClauseSyntax queryLetClause)
    {
        _letCounter++;
        return base.VisitLetClause(queryLetClause);
    }
}
