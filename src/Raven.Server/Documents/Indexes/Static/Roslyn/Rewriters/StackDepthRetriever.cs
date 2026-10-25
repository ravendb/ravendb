using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;


namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

public sealed class StackDepthRetriever : CSharpSyntaxRewriter
{
    private int _letCounter;
    private int _selectDepth;

    public int LetAndSelectDepth => _letCounter + _selectDepth;

    public void Clear()
    {
        _letCounter = 0;
        _selectDepth = 0;
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
    /// Estimates how much stack the method calls of an index function need in the generated code (RavenDB-26440).
    /// Every call adds the number of calls it is nested in, whether as their receiver or as an argument and whatever sits in between
    /// (parentheses, casts, conditionals, ...): 'x.A().B().C()' costs 0 + 1 + 2 = 3 and so does 'A(B(C(x)))', a chain of n calls costs n * (n - 1) / 2.
    /// This follows the quadratic growth of the stack frame of the generated method, which keeps the intermediate values of every pending call,
    /// and it adds up the chains of the whole function, since all of them end up in the same frame.
    /// A lambda is compiled into a method of its own with its own frame, so its body is costed separately and the largest cost of any such scope is returned.
    /// The tree is walked iteratively, so this can run on an arbitrarily deep expression.
    /// </summary>
    public static int GetMaxInvocationNestingCost(SyntaxNode expression)
    {
        var scopeCosts = new List<int> { 0 };
        var pending = new Stack<(SyntaxNode Node, int EnclosingCalls, int Scope)>();
        pending.Push((expression, 0, 0));

        while (pending.Count > 0)
        {
            var (node, enclosingCalls, scope) = pending.Pop();

            switch (node)
            {
                case InvocationExpressionSyntax:
                    scopeCosts[scope] += enclosingCalls;
                    enclosingCalls++;
                    break;

                case AnonymousFunctionExpressionSyntax:
                    scopeCosts.Add(0);
                    scope = scopeCosts.Count - 1;
                    enclosingCalls = 0;
                    break;
            }

            foreach (var child in node.ChildNodes())
                pending.Push((child, enclosingCalls, scope));
        }

        return scopeCosts.Max();
    }
}
