using System.Collections.Generic;
using System.Linq;
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
            //first change method parameters to dynamic
            var parameterList = RewriteParametersToDynamicTypes(node);

            //then create new method declaration with the dynamic parameters
            var methodWithModifiedParameterList = node.WithParameterList(parameterList);
         
            //we had a regular method declaration,
            //so we simply create new method declaration based on its body
            if (node.Body != null)
                return methodWithModifiedParameterList;

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
                        SyntaxFactory.Block(SyntaxFactory.ReturnStatement(node.ExpressionBody.Expression)),
                        null, methodWithModifiedParameterList.SemicolonToken);

            return methodDeclarationSyntax;
        }

        private static ParameterListSyntax RewriteParametersToDynamicTypes(MethodDeclarationSyntax node)
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

                sb.Append($"{DynamicString} {param.Identifier.WithoutTrivia()}");
            }

            sb.Append(')');

            return SyntaxFactory.ParseParameterList(sb.ToString());
        }
    }
}
