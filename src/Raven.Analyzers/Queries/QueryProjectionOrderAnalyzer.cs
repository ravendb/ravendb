using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Queries
{
    /// <summary>
    /// Reports RVN002 when a filtering or ordering method (Where, OrderBy, etc.) appears
    /// after a projection (ProjectInto or Select) in a RavenDB LINQ query chain.
    ///
    /// Reports RVN003 when ProjectInto is called more than once in the same chain.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryProjectionOrderAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [
            DiagnosticDescriptors.QueryFilteringAfterProjection,
            DiagnosticDescriptors.DoubleProjectInto
        ];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(Analyze, SyntaxKind.InvocationExpression);
        }

        private static void Analyze(SyntaxNodeAnalysisContext context)
        {
            var invocation = (InvocationExpressionSyntax)context.Node;
            string? methodName = SyntaxHelpers.GetMethodName(invocation);
            if (methodName == null)
                return;

            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            ExpressionSyntax receiver = memberAccess.Expression;
            ITypeSymbol? receiverType = context.SemanticModel.GetTypeInfo(receiver).Type;

            // Only act when the immediate receiver is an IRavenQueryable<T>
            if (!SyntaxHelpers.IsRavenQueryable(receiverType))
                return;

            // RVN002 — filtering/ordering after projection. The chain walk follows the receiver
            // through a local variable so a projection stored in a prior statement
            // (var p = session.Query<T>().ProjectInto<V>(); p.Where(...)) is still detected.
            if (IsPostProjectionForbiddenMethod(methodName))
            {
                InvocationExpressionSyntax? projection = FindProjectionInChain(receiver, context.SemanticModel);
                if (projection != null && ShouldReportAfterProjection(projection, methodName))
                {
                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.QueryFilteringAfterProjection,
                        memberAccess.Name.GetLocation(),
                        methodName));
                }

                return;
            }

            // RVN003 — double ProjectInto
            if (methodName == KnownTypes.ProjectIntoMethodName)
            {
                foreach (var prior in SyntaxHelpers.EnumerateInvocationChain(receiver, context.SemanticModel))
                {
                    if (SyntaxHelpers.GetMethodName(prior) == KnownTypes.ProjectIntoMethodName)
                    {
                        context.ReportDiagnostic(Diagnostic.Create(
                            DiagnosticDescriptors.DoubleProjectInto,
                            memberAccess.Name.GetLocation()));
                        return;
                    }
                }
            }
        }

        private static bool IsPostProjectionForbiddenMethod(string name) => KnownTypes.PostProjectionForbiddenMethods.Contains(name);

        /// <summary>
        /// Walks the receiver invocation chain (outer to inner) and returns the nearest ProjectInto or
        /// Select call made on an IRavenQueryable&lt;T&gt;, or null when the chain contains no projection.
        /// The nearest projection is the one whose element shape the flagged operator binds against, so
        /// it decides which forbidden set applies (see <see cref="ShouldReportAfterProjection"/>).
        /// </summary>
        private static InvocationExpressionSyntax? FindProjectionInChain(ExpressionSyntax expression, SemanticModel model)
        {
            foreach (InvocationExpressionSyntax invocation in SyntaxHelpers.EnumerateInvocationChain(expression, model))
            {
                string? name = SyntaxHelpers.GetMethodName(invocation);
                if (name != KnownTypes.ProjectIntoMethodName && name != KnownTypes.SelectMethodName)
                    continue;

                // Confirm the receiver of this projection is also IRavenQueryable<T>
                if (invocation.Expression is MemberAccessExpressionSyntax ma)
                {
                    ITypeSymbol? innerReceiverType = model.GetTypeInfo(ma.Expression).Type;
                    if (SyntaxHelpers.IsRavenQueryable(innerReceiverType))
                        return invocation;
                }
            }

            return null;
        }

        /// <summary>
        /// Decides whether an operator (<paramref name="methodName"/>) that follows
        /// <paramref name="projection"/> should be reported as RVN002.
        /// <para>
        /// ProjectInto fetches the projected member names verbatim with no source remap, so its full
        /// forbidden set applies to every operator after it. After a Select, the LINQ provider remaps a
        /// subsequent Where/OrderBy/ThenBy member path back to the source field — but only when the
        /// projected member keeps its source name. So Search/GroupBy/etc. (source- or index-shape bound)
        /// always flag after Select, and Where/OrderBy/ThenBy flag only when the Select is NOT a pure
        /// identity projection: a renamed or computed member (e.g. <c>new { Full = a + b }</c>) has no
        /// source field, so ordering/filtering by it yields wrong results server-side and must be flagged.
        /// </para>
        /// </summary>
        private static bool ShouldReportAfterProjection(InvocationExpressionSyntax projection, string methodName)
        {
            string? projectionName = SyntaxHelpers.GetMethodName(projection);

            if (projectionName != KnownTypes.SelectMethodName)
                return true; // ProjectInto — full forbidden set applies

            if (KnownTypes.PostSelectProjectionForbiddenMethods.Contains(methodName))
                return true; // Search/GroupBy/etc. are unsafe after any Select

            // Where/OrderBy/OrderByDescending/ThenBy/ThenByDescending: safe only when the Select keeps
            // every member's source name, so the operator's member path still resolves to a source field.
            return !IsPureIdentitySelectProjection(projection);
        }

        /// <summary>
        /// True when the Select projects an anonymous object in which every member is a straight
        /// pass-through of a source member under its own name — <c>new { o.A, o.B }</c> or
        /// <c>new { A = o.A }</c>. A renamed member (<c>new { X = o.A }</c>), a computed member
        /// (<c>new { X = o.A + o.B }</c>), a nested path, a non-anonymous/DTO projection, or an
        /// unreadable lambda body all make it non-identity, so the caller conservatively keeps flagging.
        /// </summary>
        private static bool IsPureIdentitySelectProjection(InvocationExpressionSyntax selectInvocation)
        {
            SeparatedSyntaxList<ArgumentSyntax> args = selectInvocation.ArgumentList.Arguments;
            if (args.Count == 0)
                return false;

            if (SyntaxHelpers.TryGetLambdaBody(args[0].Expression) is not AnonymousObjectCreationExpressionSyntax anon)
                return false;

            if (anon.Initializers.Count == 0)
                return false;

            foreach (AnonymousObjectMemberDeclaratorSyntax initializer in anon.Initializers)
            {
                // The value must be a single-hop member access off the lambda parameter (o.Member).
                if (initializer.Expression is not MemberAccessExpressionSyntax ma
                    || ma.Expression is not IdentifierNameSyntax)
                    return false;

                // Implicit name (new { o.Member }) is inferred as the member name — always identity.
                // An explicit name (new { Alias = o.Member }) is identity only when Alias == Member.
                if (initializer.NameEquals != null
                    && initializer.NameEquals.Name.Identifier.Text != ma.Name.Identifier.Text)
                    return false;
            }

            return true;
        }
    }
}
