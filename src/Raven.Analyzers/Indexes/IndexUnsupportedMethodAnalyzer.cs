using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Indexes
{
    /// <summary>
    /// Reports RVN009 when a user-defined method is called inside a Map, Reduce, or AddMap lambda
    /// of a RavenDB index class. User-defined methods cannot be translated by RavenDB's
    /// expression compiler and will cause the index to fail at deployment.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class IndexUnsupportedMethodAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.IndexUnsupportedMethodCall];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(Analyze, SyntaxKind.ClassDeclaration);
        }

        private static void Analyze(SyntaxNodeAnalysisContext context)
        {
            var classDecl = (ClassDeclarationSyntax)context.Node;
            if (classDecl.BaseList == null)
                return;

            INamedTypeSymbol? classSymbol = context.SemanticModel.GetDeclaredSymbol(classDecl);
            if (classSymbol == null)
                return;

            if (!SyntaxHelpers.IsIndexCreationTask(classSymbol))
                return;

            if (SyntaxHelpers.IsJavaScriptIndex(classSymbol))
                return;

            foreach (ConstructorDeclarationSyntax ctor in classDecl.Members.OfType<ConstructorDeclarationSyntax>())
            {
                SyntaxNode? body = ctor.GetBodyNode();
                if (body == null)
                    continue;

                AnalyzeCtorBody(context, body);
            }
        }

        private static void AnalyzeCtorBody(SyntaxNodeAnalysisContext context, SyntaxNode body)
        {
            foreach (SyntaxNode node in body.DescendantNodesAndSelf())
            {
                SyntaxNode? lambdaBody = TryGetMapReduceLambdaBody(node, context.SemanticModel);
                if (lambdaBody == null)
                    continue;

                string expressionKind = GetExpressionKind(node);

                foreach (InvocationExpressionSyntax invocation in lambdaBody.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
                {
                    ISymbol? symbol = context.SemanticModel.GetSymbolInfo(invocation).Symbol;
                    if (symbol is not IMethodSymbol method)
                        continue;

                    if (!MethodTranslatabilityHelper.IsLikelyNonTranslatable(method))
                        continue;

                    Location location = GetInvocationNameLocation(invocation);
                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.IndexUnsupportedMethodCall,
                        location,
                        method.Name,
                        expressionKind));
                }
            }
        }

        /// <summary>
        /// If <paramref name="node"/> is a Map/Reduce assignment or an AddMap/AddMapForAll invocation
        /// on the index base, returns the lambda's expression body. Otherwise returns null.
        /// </summary>
        private static SyntaxNode? TryGetMapReduceLambdaBody(SyntaxNode node, SemanticModel model)
        {
            // Map = lambda  /  Reduce = lambda  /  this.Map = lambda  /  base.Reduce = lambda  etc.
            if (node is AssignmentExpressionSyntax assignment)
            {
                SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetMapReduceLhsNameNode(assignment.Left);
                if (nameNode == null)
                    return null;

                string name = nameNode.Identifier.Text;
                if (name != KnownTypes.MapFieldName && name != KnownTypes.ReduceFieldName)
                    return null;

                ISymbol? sym = model.GetSymbolInfo(nameNode).Symbol;
                if (sym is not (IFieldSymbol or IPropertySymbol))
                    return null;

                if (!SyntaxHelpers.IsDefinedOnIndexBase(sym.ContainingType))
                    return null;

                return SyntaxHelpers.TryGetLambdaBody(assignment.Right);
            }

            // AddMap<T>(...) or AddMapForAll<T>(...)
            if (node is InvocationExpressionSyntax invocation)
            {
                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName != KnownTypes.AddMapMethodName && methodName != KnownTypes.AddMapForAllMethodName)
                    return null;

                ISymbol? sym = model.GetSymbolInfo(invocation).Symbol;
                if (sym is not IMethodSymbol method || !SyntaxHelpers.IsMultiMapBase(method.ContainingType))
                    return null;

                SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
                if (args.Count == 0)
                    return null;

                return SyntaxHelpers.TryGetLambdaBody(args[args.Count - 1].Expression);
            }

            return null;
        }

        private static string GetExpressionKind(SyntaxNode node)
        {
            if (node is AssignmentExpressionSyntax assignment
                && assignment.Left is IdentifierNameSyntax id)
            {
                return id.Identifier.Text == KnownTypes.ReduceFieldName ? "Reduce" : "Map";
            }

            return "Map";
        }

        private static Location GetInvocationNameLocation(InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                return memberAccess.Name.GetLocation();
            if (invocation.Expression is IdentifierNameSyntax identifier)
                return identifier.GetLocation();
            return invocation.GetLocation();
        }
    }
}
