using System.Collections.Generic;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class MethodDynamicParametersRewriter : CSharpSyntaxRewriter
    {
        public SemanticModel SemanticModel { get; internal set; }

        private const string DynamicString = "dynamic";

        private const string String = "string";

        private const string SystemNamespacePrefix = "System.";

        private const string IEnumerableString = "System.Collections.IEnumerable";

        private const string DynamicArrayString = "DynamicArray";

        private INamedTypeSymbol IEnumerableSymbol; 

        public override SyntaxNode VisitMethodDeclaration(MethodDeclarationSyntax node)
        {
            //first change method parameters to dynamic
            var parameterList = RewriteParametersToDynamicTypes(node, out var statements);

            //then create new method declaration with the dynamic parameters
            var methodWithModifiedParameterList = node.WithParameterList(parameterList);

            //we had a regular method declaration,
            //so we simply create new method declaration based on its body
            if (node.Body != null)
            {
                if (statements.Count == 0)
                    return methodWithModifiedParameterList;
                return methodWithModifiedParameterList.WithBody(methodWithModifiedParameterList.Body
                    .WithStatements(methodWithModifiedParameterList.Body.Statements.InsertRange(0, statements)));
            }

            statements.Add(SyntaxFactory.ReturnStatement(node.ExpressionBody.Expression));

            //if node.Body == null then we have an arrow function method,
            //so we need to create new method declaration from scratch.
            //The following line creates regular method declaration based on the arrow method
            var methodDeclarationSyntax = 
                    SyntaxFactory.MethodDeclaration(methodWithModifiedParameterList.AttributeLists,
                        methodWithModifiedParameterList.Modifiers,
                        methodWithModifiedParameterList.ReturnType,
                        methodWithModifiedParameterList.ExplicitInterfaceSpecifier,
                        methodWithModifiedParameterList.Identifier,
                        methodWithModifiedParameterList.TypeParameterList,
                        methodWithModifiedParameterList.ParameterList,
                        methodWithModifiedParameterList.ConstraintClauses,
                        SyntaxFactory.Block(statements),
                        null, methodWithModifiedParameterList.SemicolonToken);

            return methodDeclarationSyntax;
        }

        private ParameterListSyntax RewriteParametersToDynamicTypes(MethodDeclarationSyntax node, out List<StatementSyntax> statements)
        {
            IEnumerableSymbol = SemanticModel.Compilation.GetTypeByMetadataName(IEnumerableString);
            statements = new List<StatementSyntax>();

            var first = true;
            var sb = new StringBuilder();
            sb.Append('(');

            foreach (var param in node.ParameterList.Parameters)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.Append(", ");
                }

                var typeStr = param.Type.ToString();
                if (typeStr == DynamicString || typeStr == String)
                {
                    sb.Append(param);
                    continue;
                }

                var symbol = SemanticModel.GetDeclaredSymbol(param);

                if (symbol.Type.AllInterfaces.Contains(IEnumerableSymbol))
                {
                    // change to DynamicArray
                    var identifier = param.Identifier.WithoutTrivia();
                    sb.Append($"{DynamicString} d_{identifier}");
                    statements.Add(SyntaxFactory.ParseStatement(
                        $"var {identifier} = new {DynamicArrayString}(d_{identifier});"));
                    continue;
                }

                if (symbol.Type.ToString().StartsWith(SystemNamespacePrefix) == false)
                {
                    // change to dynamic
                    sb.Append($"{DynamicString} {param.Identifier.WithoutTrivia()}");
                    continue;
                }

                // keep original type
                sb.Append(param);
            }

            sb.Append(')');

            return SyntaxFactory.ParseParameterList(sb.ToString());
        }
    }
}
