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
                CheckMultiMapAddMapCalls(context, classDecl, classSymbol);
            else
                CheckRegularIndexMapAssignment(context, classDecl, classSymbol);
        }

        private static void CheckRegularIndexMapAssignment(
            SyntaxNodeAnalysisContext context,
            ClassDeclarationSyntax classDecl,
            INamedTypeSymbol classSymbol)
        {
            // RVN004 — no constructor in this class OR any user-defined base class assigns Map.
            // A base index class that defines a shared Map (the common abstract-base pattern) must
            // count, so the search walks the inheritance chain rather than only this declaration.
            if (IndexInheritanceInspector.FindMapAssignmentInChain(classSymbol, context.Compilation) != IndexChainSearch.NotFound)
                return;

            context.ReportDiagnostic(Diagnostic.Create(
                DiagnosticDescriptors.IndexMissingMapAssignment,
                classDecl.Identifier.GetLocation(),
                classDecl.Identifier.Text));
        }

        private static void CheckMultiMapAddMapCalls(
            SyntaxNodeAnalysisContext context,
            ClassDeclarationSyntax classDecl,
            INamedTypeSymbol classSymbol)
        {
            (int totalAddMapCount, bool anyInLoop, bool unknown) =
                IndexInheritanceInspector.CountAddMapInChain(classSymbol, context.Compilation);

            // A base class is metadata-only; its AddMap calls are invisible, so don't report.
            if (unknown)
                return;

            if (totalAddMapCount == 0)
            {
                // RVN005 — no AddMap in any constructor (this class or a user-defined base)
                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.MultiMapIndexMissingAddMap,
                    classDecl.Identifier.GetLocation(),
                    classDecl.Identifier.Text));
            }
            else if (totalAddMapCount == 1 && anyInLoop == false)
            {
                // RVN006 — exactly one AddMap call site; a regular index would suffice. Suppressed
                // when that call sits in a loop, where it registers a map per iteration at runtime
                // and the multi-map base is genuinely required.
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
                SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetSimpleMemberName(assignment.Left);
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

    }
}
