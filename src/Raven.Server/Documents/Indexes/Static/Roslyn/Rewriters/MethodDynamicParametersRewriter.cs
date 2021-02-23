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
        public SemanticModel SemanticModel
        {
            get => _semanticModel;
            internal set
            {
                _semanticModel = value;
                IEnumerableSymbol = value.Compilation.GetTypeByMetadataName(IEnumerableString);
            }
        }

        private const string DynamicString = "dynamic";
        
        private const string SystemNamespacePrefix = "System.";

        private const string IEnumerableString = "System.Collections.IEnumerable";

        private const string DynamicArrayString = "DynamicArray";
      
        private static readonly TypeSyntax DynamicArrayTypeSyntax = SyntaxFactory.ParseTypeName(DynamicArrayString);

        private static readonly IdentifierNameSyntax DynamicIdentifier = 
            SyntaxFactory.IdentifierName(DynamicString);

        private static readonly IdentifierNameSyntax VarIdentifier =
            SyntaxFactory.IdentifierName("var");

        private static readonly GenericNameSyntax IEnumerableDynamicNameSyntax =
            SyntaxFactory.GenericName(SyntaxFactory.Identifier("IEnumerable"),
                SyntaxFactory.TypeArgumentList(new SeparatedSyntaxList<TypeSyntax>().Add(DynamicIdentifier)));

        private INamedTypeSymbol IEnumerableSymbol;
        private SemanticModel _semanticModel;

        public override SyntaxNode VisitMethodDeclaration(MethodDeclarationSyntax node)
        {
            var isPublic = node.Modifiers.Any(x => x.Kind() == SyntaxKind.PublicKeyword);
            if (isPublic == false)
                return node; //need to only modify public methods 
            
            //first change method parameters to dynamic
            var parameterList = RewriteParametersToDynamicTypes(node, out var statements);

            //then create new method declaration with the dynamic parameters
            var modifiedMethod = node.WithParameterList(parameterList);

            if (ShouldModifyReturnType(node, out var newReturnType))
            {
                // change return type to dynamic (or IEnumerable<dynamic>)
                modifiedMethod = modifiedMethod.ReplaceNode(modifiedMethod.ReturnType, newReturnType);

                if (newReturnType == IEnumerableDynamicNameSyntax)
                {
                    // need to modify the return statements
                    // e.g. 'return x' =>  'return new DynamicArray(x)' 

                    modifiedMethod = ModifyReturnStatements(modifiedMethod);
                }
            }

            //if we had a regular method declaration,
            //simply create new method declaration based on its body
            if (node.Body != null)
            {
                if (statements.Count == 0)
                    return modifiedMethod;
                return modifiedMethod.WithBody(modifiedMethod.Body
                    .WithStatements(modifiedMethod.Body.Statements.InsertRange(0, statements)));
            }

            statements.Add(SyntaxFactory.ReturnStatement(node.ExpressionBody.Expression));

            //if node.Body == null then we have an arrow function method,
            //so we need to create new method declaration from scratch.
            //The following line creates regular method declaration based on the arrow method
            var methodDeclarationSyntax = 
                    SyntaxFactory.MethodDeclaration(modifiedMethod.AttributeLists,
                        modifiedMethod.Modifiers,
                        modifiedMethod.ReturnType,
                        modifiedMethod.ExplicitInterfaceSpecifier,
                        modifiedMethod.Identifier,
                        modifiedMethod.TypeParameterList,
                        modifiedMethod.ParameterList,
                        modifiedMethod.ConstraintClauses,
                        SyntaxFactory.Block(statements),
                        null, modifiedMethod.SemicolonToken);

            return methodDeclarationSyntax;
        }

        private static MethodDeclarationSyntax ModifyReturnStatements(MethodDeclarationSyntax method)
        {
            var modifiedStatements = method.Body.Statements;

            for (var index = 0; index < method.Body.Statements.Count; index++)
            {
                var statement = method.Body.Statements[index];

                StatementSyntax modifiedStatement;
                if (statement is ReturnStatementSyntax returnStatementSyntax)
                {
                    modifiedStatement = ModifySingleStatement(returnStatementSyntax, returnStatementSyntax);
                    modifiedStatements = modifiedStatements.RemoveAt(index).Insert(index, modifiedStatement);
                    continue;
                }

                modifiedStatement = statement;
                var returnStatements = statement.DescendantNodes().OfType<ReturnStatementSyntax>();

                foreach (var returnStatement in returnStatements)
                {
                    modifiedStatement = ModifySingleStatement(returnStatement, modifiedStatement);
                }

                modifiedStatements = modifiedStatements.RemoveAt(index).Insert(index, modifiedStatement);

            }

            return method.WithBody(method.Body.WithStatements(modifiedStatements));

        }

        private static StatementSyntax ModifySingleStatement(ReturnStatementSyntax returnStatement, StatementSyntax statement)
        {
            var newReturnStatement = CreateNewReturnStatement(returnStatement);
            return statement.ReplaceNode(returnStatement, newReturnStatement);
        }

        private static ReturnStatementSyntax CreateNewReturnStatement(ReturnStatementSyntax returnStatement)
        {
            var argumentList = new SeparatedSyntaxList<ArgumentSyntax>();
            argumentList = argumentList.Add(SyntaxFactory.Argument(returnStatement.Expression));

            var objectCreationExpression = SyntaxFactory.ObjectCreationExpression(
                type: DynamicArrayTypeSyntax,
                argumentList: SyntaxFactory.ArgumentList(argumentList),
                initializer: null);

            var newReturnStatement = SyntaxFactory.ReturnStatement(objectCreationExpression);

            return newReturnStatement;
        }

        private bool ShouldModifyReturnType(MethodDeclarationSyntax node, out SyntaxNode returnType)
        {
            var typeSymbol = SemanticModel.GetTypeInfo(node.ReturnType).Type;
            return ShouldModifyType(typeSymbol, out returnType);
        }

        private bool ShouldModifyType(ITypeSymbol typeSymbol, out SyntaxNode newType)
        {
            newType = default;
            if (typeSymbol.SpecialType == SpecialType.System_String ||
                typeSymbol.ToString() == DynamicString)
            {
                return false;
            }

            if (typeSymbol.AllInterfaces.Contains(IEnumerableSymbol))
            {
                newType = IEnumerableDynamicNameSyntax;
                return true;
            }

            switch (typeSymbol.SpecialType)
            {
                case SpecialType.None:
                {
                    if (typeSymbol.ToString().StartsWith(SystemNamespacePrefix))
                    {
                        // keep original type
                        return false;
                    }

                    break;
                }
                case SpecialType.System_SByte:
                case SpecialType.System_Int16:
                case SpecialType.System_Int32:
                case SpecialType.System_Int64:
                case SpecialType.System_Byte:
                case SpecialType.System_UInt16:
                case SpecialType.System_UInt32:
                case SpecialType.System_UInt64:
                case SpecialType.System_Single:
                case SpecialType.System_Double:
                case SpecialType.System_Decimal:
                case SpecialType.System_Object:
                {
                    break;
                }
                default:
                {
                    // keep original type
                    return false;
                }
            }

            // change to dynamic
            newType = DynamicIdentifier;
            return true;
        }

        private ParameterListSyntax RewriteParametersToDynamicTypes(MethodDeclarationSyntax node, out List<StatementSyntax> statements)
        {
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

                var symbol = SemanticModel.GetDeclaredSymbol(param).Type;
                if (ShouldModifyType(symbol, out var newType))
                {
                    if (newType == IEnumerableDynamicNameSyntax)
                    {
                        // change to DynamicArray
                        var identifier = param.Identifier.WithoutTrivia();
                        sb.Append($"{DynamicString} d_{identifier}");

                        var newStatement = CreateDynamicArrayDeclarationStatement(identifier);
                        statements.Add(newStatement);

                        continue;
                    }

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

        private static LocalDeclarationStatementSyntax CreateDynamicArrayDeclarationStatement(SyntaxToken identifier)
        {
            var argumentList = new SeparatedSyntaxList<ArgumentSyntax>();
            argumentList = argumentList.Add(SyntaxFactory.Argument(SyntaxFactory.IdentifierName($"d_{identifier}")));

            var objectCreationExpression = SyntaxFactory.ObjectCreationExpression(
                type: DynamicArrayTypeSyntax,
                argumentList: SyntaxFactory.ArgumentList(argumentList),
                initializer: null);

            var variables = new SeparatedSyntaxList<VariableDeclaratorSyntax>().Add(
                SyntaxFactory.VariableDeclarator(
                    identifier: identifier, 
                    argumentList: null, 
                    initializer: SyntaxFactory.EqualsValueClause(objectCreationExpression)));

            var newStatement = SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(VarIdentifier, variables));

            return newStatement;
        }
    }
}
