using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;


namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

public sealed class StackDepthRetriever : CSharpSyntaxRewriter
{
    private int _letCounter;
    private int _selectDepth;
    private int _maxInvocationChainDepth;

    public int LetAndSelectDepth => _letCounter + _selectDepth;

    public int MaxInvocationChainDepth => _maxInvocationChainDepth;

    public void Clear()
    {
        _letCounter = 0;
        _selectDepth = 0;
        _maxInvocationChainDepth = 0;
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

    /// <summary>
    /// Finds the longest chain of method calls on a single receiver, e.g. 'x.Concat(a).Where(b).Select(c)' has a depth of 3.
    /// The tree is walked iteratively, so this can run on an arbitrarily deep expression before any recursive rewriter touches it.
    /// </summary>
    public void VisitInvocationChains(SyntaxNode expression)
    {
        foreach (var invocation in expression.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
        {
            // count each chain once, starting from its outermost call
            if (IsReceiverOfAnotherCall(invocation))
                continue;

            var depth = CountChainDepth(invocation);
            if (depth > _maxInvocationChainDepth)
                _maxInvocationChainDepth = depth;
        }
    }

    private static bool IsReceiverOfAnotherCall(InvocationExpressionSyntax invocation)
    {
        // 'invocation' is the part before the dot in 'invocation.Method(...)'
        return invocation.Parent is MemberAccessExpressionSyntax { Parent: InvocationExpressionSyntax };
    }

    private static int CountChainDepth(InvocationExpressionSyntax outermostCall)
    {
        // a chain like 'x.Concat(a).Where(b)' is parsed from the outside in:
        //   Invocation( MemberAccess( Invocation( MemberAccess( x, Concat ) ), Where ) )
        // so we start at the outermost call and walk down through the receiver (the expression before the dot)
        // for as long as the receiver is itself a method call, counting one level per call
        var depth = 0;
        SyntaxNode current = outermostCall;

        while (current is InvocationExpressionSyntax { Expression: MemberAccessExpressionSyntax memberAccess })
        {
            depth++;
            current = memberAccess.Expression;
        }

        return depth;
    }
}
