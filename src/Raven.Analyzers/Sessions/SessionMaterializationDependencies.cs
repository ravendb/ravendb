using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Sessions
{
    /// <summary>
    /// The derived set is method-wide and flow-insensitive: once a symbol is fed by a materialized
    /// result it stays derived for the whole block. Reassigning that same symbol to an independent
    /// value later does not "un-derive" it, so a Load reading the reassigned symbol is treated as
    /// dependent and not flagged. That is a missed batching hint (the safe direction for this
    /// advisory rule) on an unusual scratch-variable-reuse pattern; proper handling would need
    /// per-position dataflow this two-pass walk does not carry.
    /// </summary>
    public sealed class SessionMaterializationDependencies
    {
        private readonly SemanticModel _model;
        private readonly HashSet<ISymbol> _derivedSet = new(SymbolEqualityComparer.Default);

        public SessionMaterializationDependencies(SemanticModel model, SyntaxNode codeBlock)
        {
            _model = model;
            new DerivedSymbolCollector(model, _derivedSet).Visit(codeBlock);
        }

        public bool IsIndependentLoadArgument(ArgumentSyntax arg)
        {
            ExpressionSyntax expr = arg.Expression;

            // Literal constants are independent
            if (expr is LiteralExpressionSyntax)
                return true;

            // Simple identifiers: check if they resolve to parameter, field, or independent local
            if (expr is IdentifierNameSyntax id)
            {
                ISymbol? symbol = _model.GetSymbolInfo(id).Symbol;

                // Parameters, fields and properties are context-provided — unless a prior materialized
                // result flowed into them (id = order.CustomerId, this._customerId = order.CustomerId),
                // in which case a Load reading them genuinely depends on the earlier server call and
                // must not be batched.
                if (symbol is IParameterSymbol or IFieldSymbol or IPropertySymbol)
                    return !_derivedSet.Contains(symbol);

                // Local variables: check if directly materialization-derived
                if (symbol is ILocalSymbol local)
                {
                    if (_derivedSet.Contains(local))
                        return false; // Directly derived from materialization

                    // Check the local's initializer for dependency on any materialization-derived symbol
                    VariableDeclaratorSyntax? declarator = FindDeclarator(local);
                    if (declarator?.Initializer?.Value is ExpressionSyntax initExpr)
                        return IsSimpleContextExpr(initExpr);

                    return false; // Can't determine, be conservative
                }

                return false; // Other symbol kinds
            }

            // Complex expressions: be conservative, assume dependent
            return false;
        }

        // True when any part of the query invocation (chain receiver, predicate/projection
        // lambdas, indexer/id arguments) references a symbol that a prior materialized session
        // call produced. Such a query depends on that result and cannot be folded into the same
        // lazy multi-get, so it must be excluded from the batchable set — the query counterpart
        // of IsIndependentLoadArgument for loads. Symbols are compared (not identifier text) so an
        // unrelated lambda parameter that merely shares a name does not trigger a false exclusion.
        public bool DependsOnMaterializedResult(InvocationExpressionSyntax invocation)
        {
            foreach (IdentifierNameSyntax id in invocation.DescendantNodes().OfType<IdentifierNameSyntax>())
            {
                ISymbol? symbol = _model.GetSymbolInfo(id).Symbol;
                if (symbol is ILocalSymbol or IParameterSymbol or IFieldSymbol or IPropertySymbol
                    && _derivedSet.Contains(symbol))
                {
                    return true;
                }
            }

            return false;
        }

        private bool IsSimpleContextExpr(ExpressionSyntax expr)
        {
            // Literals are simple and context-provided
            if (expr is LiteralExpressionSyntax)
                return true;

            // Simple identifiers: check if they resolve to parameter, field, or non-derived local
            if (expr is IdentifierNameSyntax id)
            {
                ISymbol? symbol = _model.GetSymbolInfo(id).Symbol;

                // A parameter/field/property/local is context-provided only when it was not fed by a
                // prior materialized result (see the derived-set tracking in DerivedSymbolCollector).
                if (symbol is IParameterSymbol or IFieldSymbol or IPropertySymbol or ILocalSymbol)
                    return !_derivedSet.Contains(symbol);

                return false;
            }

            // Complex expressions are assumed dependent
            return false;
        }

        private static VariableDeclaratorSyntax? FindDeclarator(ILocalSymbol local)
        {
            if (local.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                return null;

            return local.DeclaringSyntaxReferences[0].GetSyntax() as VariableDeclaratorSyntax;
        }

        private sealed class DerivedSymbolCollector(SemanticModel model, HashSet<ISymbol> derivedSet) : CSharpSyntaxWalker
        {
            // Dependency tracking uses the BROAD set: any materializer (including First/Single/Count/…)
            // produces a local that downstream operations may depend on, so all of them must be tracked
            // even though the detection pass only flags the lazy-batchable subset.
            private static readonly HashSet<string> QueryMaterializingMethods = KnownTypes.SessionMaterializingMethods;

            public override void VisitLocalDeclarationStatement(LocalDeclarationStatementSyntax node)
            {
                foreach (VariableDeclaratorSyntax declarator in node.Declaration.Variables)
                {
                    if (declarator.Initializer?.Value is ExpressionSyntax value
                        && IsDerivedInitializer(value)
                        && model.GetDeclaredSymbol(declarator) is ISymbol symbol)
                    {
                        derivedSet.Add(symbol);
                    }
                }

                base.VisitLocalDeclarationStatement(node);
            }

            // A parameter, field, property, or local (re)assigned from a materialized server call — or from
            // an expression that reads an already-derived symbol — becomes derived itself, so a later Load id
            // or query predicate that reads it genuinely depends on the earlier round-trip and must not be
            // batched. Handling plain assignments (not just declarations) closes the field-mediated
            // dependency path: var o = session.Load<Order>(id); this._customerId = o.CustomerId;
            // session.Load<Customer>(_customerId);
            public override void VisitAssignmentExpression(AssignmentExpressionSyntax node)
            {
                ISymbol? target = model.GetSymbolInfo(node.Left).Symbol;
                if (node.IsKind(SyntaxKind.SimpleAssignmentExpression)
                    && target is ILocalSymbol or IParameterSymbol or IFieldSymbol or IPropertySymbol
                    && IsDerivedInitializer(node.Right))
                {
                    derivedSet.Add(target);
                }

                base.VisitAssignmentExpression(node);
            }

            private bool IsDerivedInitializer(ExpressionSyntax? value) =>
                IsMaterializingSessionCall(value) || ReferencesDerivedSymbol(value);

            // True when <paramref name="value"/> is a session Load or a materializing query call
            // (unwrapping a leading await), on a RavenDB session / queryable.
            private bool IsMaterializingSessionCall(ExpressionSyntax? value)
            {
                ExpressionSyntax? expr = value is AwaitExpressionSyntax awaitExpr ? awaitExpr.Expression : value;
                if (expr is not InvocationExpressionSyntax invocation
                    || invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                {
                    return false;
                }

                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName == null)
                    return false;

                if (QueryMaterializingMethods.Contains(methodName) && SyntaxHelpers.IsRavenQueryableRooted(memberAccess.Expression, model))
                    return true;

                ITypeSymbol? receiverType = model.GetTypeInfo(memberAccess.Expression).Type;
                return (methodName == KnownTypes.LoadMethodName || methodName == KnownTypes.LoadAsyncMethodName)
                       && SyntaxHelpers.IsSessionType(receiverType);
            }

            private bool ReferencesDerivedSymbol(ExpressionSyntax? value)
            {
                if (value == null)
                    return false;

                foreach (IdentifierNameSyntax id in value.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>())
                {
                    ISymbol? symbol = model.GetSymbolInfo(id).Symbol;
                    if (symbol != null && derivedSet.Contains(symbol))
                        return true;
                }

                return false;
            }

            public override void VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node)
            {
                // Skip nested lambdas
            }

            public override void VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node)
            {
                // Skip nested lambdas
            }

            public override void VisitAnonymousMethodExpression(AnonymousMethodExpressionSyntax node)
            {
                // Skip anonymous methods
            }

            public override void VisitLocalFunctionStatement(LocalFunctionStatementSyntax node)
            {
                // Skip local functions
            }
        }
    }
}
