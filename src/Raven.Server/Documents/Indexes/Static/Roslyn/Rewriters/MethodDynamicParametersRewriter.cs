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

        private const string SystemNamespacePrefix = "System.";

        private const string IEnumerableString = "System.Collections.IEnumerable";

        private INamedTypeSymbol IEnumerableSymbol; 

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

        private ParameterListSyntax RewriteParametersToDynamicTypes(MethodDeclarationSyntax node)
        {
            IEnumerableSymbol = SemanticModel.Compilation.GetTypeByMetadataName(IEnumerableString);

            var originalParameters = node.ParameterList.Parameters;
            var first = true;
            var sb = new StringBuilder();
            sb.Append('(');

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

                var symbol = SemanticModel.GetDeclaredSymbol(param);
                var typeStr = symbol.Type.ToString();

                if (typeStr.StartsWith(SystemNamespacePrefix) == false)
                {
                    // change to dynamic
                    sb.Append($"{DynamicString} {param.Identifier.WithoutTrivia()}");
                    continue;
                }

                if (param.Type is GenericNameSyntax genericNameSyntax && 
                    symbol.Type.AllInterfaces.Contains(IEnumerableSymbol))
                {
                    // handle generic type arguments of collection

                    var any = false;
                    var newArgsBuilder = ChangeGenericTypeArgumentsToDynamicIfNeeded(genericNameSyntax, ref any);

                    if (any)
                    {
                        // we changed some of the generic argument types to dynamic,
                        // create a new parameter with the modified type-argument list
                        var newParam = $"{genericNameSyntax.Identifier.Text}<{newArgsBuilder}> {param.Identifier.WithoutTrivia()}";
                        sb.Append(newParam);
                        continue;
                    }                 
                }

                // no need to change to dynamic, keep original type
                sb.Append(param);
            }

            sb.Append(')');

            return SyntaxFactory.ParseParameterList(sb.ToString());
        }

        private StringBuilder ChangeGenericTypeArgumentsToDynamicIfNeeded(GenericNameSyntax gns, ref bool anyChanges)
        {
            var first = true;
            var sb = new StringBuilder();

            foreach (var arg in gns.TypeArgumentList.Arguments)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.Append(", ");
                }

                var genericTypeSymbol = SemanticModel.GetTypeInfo(arg).Type;

                if (genericTypeSymbol.ToString().StartsWith(SystemNamespacePrefix))
                {
                    if (arg is GenericNameSyntax innerGenericNameSyntax &&
                        genericTypeSymbol.AllInterfaces.Contains(IEnumerableSymbol))
                    {
                        // recursive call
                        var newBuilder = ChangeGenericTypeArgumentsToDynamicIfNeeded(innerGenericNameSyntax, ref anyChanges);
                        var newGenericType = $"{innerGenericNameSyntax.Identifier.Text}<{newBuilder}>";
                        sb.Append(newGenericType);
                    }
                    else
                    {
                        // keep original type
                        sb.Append(arg);
                    }
                }
                else
                {
                    // change to dynamic
                    anyChanges = true;
                    sb.Append(DynamicString);
                }
            }

            return sb;
        }
    }
}
