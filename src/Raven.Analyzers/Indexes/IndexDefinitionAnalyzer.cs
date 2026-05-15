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
    /// Reports RVN001 when Map or Reduce is assigned in a method (not a constructor).
    /// Reports RVN004 when a regular index class has no constructor that assigns the Map property.
    /// Reports RVN005 when a multi-map index class has no constructor that calls AddMap.
    /// Reports RVN006 when a multi-map index class has exactly one AddMap call in all constructors.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class IndexDefinitionAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [
            DiagnosticDescriptors.IndexMapAssignedOutsideCtor,
            DiagnosticDescriptors.IndexMissingMapAssignment,
            DiagnosticDescriptors.MultiMapIndexMissingAddMap,
            DiagnosticDescriptors.MultiMapIndexSingleAddMap
        ];

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

            // RVN001 — Map/Reduce assigned in a regular method (not a constructor)
            foreach (MethodDeclarationSyntax method in classDecl.Members.OfType<MethodDeclarationSyntax>())
                CheckForMapReduceAssignmentsInMethod(context, method);

            bool isMultiMap = SyntaxHelpers.IsMultiMapIndexCreationTask(classSymbol);

            if (isMultiMap)
                CheckMultiMapAddMapCalls(context, classDecl);
            else
                CheckRegularIndexMapAssignment(context, classDecl);
        }

        private static void CheckRegularIndexMapAssignment(
            SyntaxNodeAnalysisContext context,
            ClassDeclarationSyntax classDecl)
        {
            // RVN004 — No constructor assigns Map
            bool mapAssignedInCtor = classDecl.Members
                .OfType<ConstructorDeclarationSyntax>()
                .Any(ctor => ctor.GetBodyNode() is SyntaxNode body && ContainsMapAssignment(body, context.SemanticModel));

            if (!mapAssignedInCtor)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.IndexMissingMapAssignment,
                    classDecl.Identifier.GetLocation(),
                    classDecl.Identifier.Text));
            }
        }

        private static void CheckMultiMapAddMapCalls(
            SyntaxNodeAnalysisContext context,
            ClassDeclarationSyntax classDecl)
        {
            int totalAddMapCount = classDecl.Members
                .OfType<ConstructorDeclarationSyntax>()
                .Sum(ctor => ctor.GetBodyNode() is SyntaxNode body ? CountAddMapInvocations(body, context.SemanticModel) : 0);

            if (totalAddMapCount == 0)
            {
                // RVN005 — no AddMap in any constructor
                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.MultiMapIndexMissingAddMap,
                    classDecl.Identifier.GetLocation(),
                    classDecl.Identifier.Text));
            }
            else if (totalAddMapCount == 1)
            {
                // RVN006 — exactly one AddMap; a regular index would suffice
                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.MultiMapIndexSingleAddMap,
                    classDecl.Identifier.GetLocation(),
                    classDecl.Identifier.Text));
            }
        }

        private static void CheckForMapReduceAssignmentsInMethod(
            SyntaxNodeAnalysisContext context,
            MethodDeclarationSyntax method)
        {
            SyntaxNode? body = method.GetBodyNode();
            if (body == null)
                return;

            foreach (AssignmentExpressionSyntax assignment in
                body.DescendantNodesAndSelf().OfType<AssignmentExpressionSyntax>())
            {
                SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetMapReduceLhsNameNode(assignment.Left);
                if (nameNode == null)
                    continue;

                string name = nameNode.Identifier.Text;
                if (name != KnownTypes.MapFieldName && name != KnownTypes.ReduceFieldName)
                    continue;

                // Confirm it resolves to the actual Map/Reduce property on an index base class
                ISymbol? symbol = context.SemanticModel.GetSymbolInfo(nameNode).Symbol;
                if (symbol is (IFieldSymbol or IPropertySymbol)
                    && SyntaxHelpers.IsDefinedOnIndexBase(symbol.ContainingType))
                {
                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.IndexMapAssignedOutsideCtor,
                        assignment.GetLocation(),
                        name));
                }
            }
        }

        private static bool ContainsMapAssignment(SyntaxNode node, SemanticModel model)
        {
            foreach (AssignmentExpressionSyntax assignment in
                node.DescendantNodesAndSelf().OfType<AssignmentExpressionSyntax>())
            {
                SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetMapReduceLhsNameNode(assignment.Left);
                if (nameNode == null || nameNode.Identifier.Text != KnownTypes.MapFieldName)
                    continue;

                ISymbol? symbol = model.GetSymbolInfo(nameNode).Symbol;
                if (symbol is (IFieldSymbol or IPropertySymbol)
                    && SyntaxHelpers.IsDefinedOnIndexBase(symbol.ContainingType))
                {
                    return true;
                }
            }

            return false;
        }

        private static int CountAddMapInvocations(SyntaxNode node, SemanticModel model)
        {
            int count = 0;

            foreach (InvocationExpressionSyntax invocation in
                node.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
            {
                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName != KnownTypes.AddMapMethodName && methodName != KnownTypes.AddMapForAllMethodName)
                    continue;

                // Confirm the method resolves to the AddMap defined on a multi-map index base class
                ISymbol? symbol = model.GetSymbolInfo(invocation).Symbol;
                if (symbol is IMethodSymbol method
                    && SyntaxHelpers.IsMultiMapBase(method.ContainingType))
                {
                    count++;
                }
            }

            return count;
        }
    }
}
