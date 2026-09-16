using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;

namespace Corax.Querying.Planning;

public static class CSharpFormatter
{
    // Create an ad-hoc workspace to gain access to the formatting engine
    private static readonly AdhocWorkspace Workspace = new();

    public static string Format(string sourceCode)
    {
        if (string.IsNullOrEmpty(sourceCode))
            return sourceCode;

        try
        {
            var tree = CSharpSyntaxTree.ParseText(sourceCode);
            var root = tree.GetCompilationUnitRoot();

            // Use the formatting engine (default smart indent keeps labels indented)
            var formattedNode = Formatter.Format(root, Workspace);
            return formattedNode.ToFullString();
        }
        catch
        {
            return sourceCode;
        }
    }
}
