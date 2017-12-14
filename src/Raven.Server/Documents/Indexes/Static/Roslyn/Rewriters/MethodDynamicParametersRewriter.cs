using System.Collections.Generic;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class MethodDynamicParametersRewriter : CSharpSyntaxRewriter
    {
        public static readonly MethodDynamicParametersRewriter Instance = new MethodDynamicParametersRewriter();

        private MethodDynamicParametersRewriter()
        {
        }

        private const string DynamicString = "dynamic";

        public override SyntaxNode VisitMethodDeclaration(MethodDeclarationSyntax node)
        {
            var originalParameters = node.ParameterList.Parameters;
            var sb = new StringBuilder();
            sb.Append('(');
            var first = true;
            foreach (var param in originalParameters)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.Append(", ");
                }
                if (param.Type.ToString() == DynamicString)
                {
                    sb.Append(param);
                    continue;
                }
                sb.Append($"{DynamicString} d_{param.Identifier.WithoutTrivia()}");
            }
            sb.Append(')');
            var modifiedParameterList = node.WithParameterList(SyntaxFactory.ParseParameterList(sb.ToString()));
            var statements = new List<StatementSyntax>();
            foreach (var param in originalParameters)
            {
                if (param.Type.ToString() == DynamicString)
                    continue;
                statements.Add(SyntaxFactory.ParseStatement($"{param.Type} {param.Identifier.WithoutTrivia()} = ({param.Type})d_{param.Identifier.WithoutTrivia()};"));
            }
            if (statements.Count == 0)
                return modifiedParameterList;
            return modifiedParameterList.WithBody(modifiedParameterList.Body.WithStatements(modifiedParameterList.Body.Statements.InsertRange(0, statements)));
        }
    }
}
