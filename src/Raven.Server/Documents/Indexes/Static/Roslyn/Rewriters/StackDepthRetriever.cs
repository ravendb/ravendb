using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;


namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

public class StackDepthRetriever : CSharpSyntaxRewriter
{
    private int _letCounter;

    private int _selectDepth;

    public int StackSize => _letCounter + _selectDepth;

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
}
