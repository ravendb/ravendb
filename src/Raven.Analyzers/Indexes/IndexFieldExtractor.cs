using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Indexes
{
    internal enum IndexFieldInspection
    {
        Ok,
        BailCannotAnalyze
    }

    internal record struct IndexFieldSet(IndexFieldInspection Status, ImmutableHashSet<string> Fields)
    {
        public static readonly IndexFieldSet Bail = new(IndexFieldInspection.BailCannotAnalyze, ImmutableHashSet<string>.Empty);
    }

    /// <summary>
    /// Extracts the set of projected field names from a C#-based RavenDB index class.
    /// Returns <see cref="IndexFieldInspection.BailCannotAnalyze"/> when the index uses
    /// dynamic field creation, JavaScript, or patterns that cannot be statically analyzed.
    /// </summary>
    internal static class IndexFieldExtractor
    {
        /// <summary>
        /// Extracts the field names projected by the index <c>Map</c>. <c>StoreAllFields</c> affects
        /// field <em>storage</em>, not which fields the Map projects, so it does not influence this set.
        /// </summary>
        public static IndexFieldSet Extract(INamedTypeSymbol indexClass, Compilation compilation)
        {
            // Must have source syntax in this compilation
            if (indexClass.DeclaringSyntaxReferences.IsEmpty)
                return IndexFieldSet.Bail;

            // JS-based indexes cannot be statically analyzed
            if (SyntaxHelpers.IsJavaScriptIndex(indexClass))
                return IndexFieldSet.Bail;

            var allFields = new HashSet<string>(System.StringComparer.Ordinal);

            foreach (SyntaxReference syntaxRef in indexClass.DeclaringSyntaxReferences)
            {
                if (syntaxRef.GetSyntax() is not ClassDeclarationSyntax classDecl)
                    continue;

                SemanticModel model = compilation.GetSemanticModel(classDecl.SyntaxTree);

                foreach (ConstructorDeclarationSyntax ctor in classDecl.Members
                    .OfType<ConstructorDeclarationSyntax>())
                {
                    SyntaxNode? ctorBody = ctor.GetBodyNode();
                    if (ctorBody == null)
                        continue;

                    // Bail if the constructor uses dynamic field creation
                    if (SyntaxHelpers.ContainsDynamicFieldCalls(ctorBody))
                        return IndexFieldSet.Bail;

                    IndexFieldInspection result = ExtractFromCtorBody(ctorBody, model, allFields);
                    if (result == IndexFieldInspection.BailCannotAnalyze)
                        return IndexFieldSet.Bail;
                }
            }

            return new IndexFieldSet(IndexFieldInspection.Ok, allFields.ToImmutableHashSet());
        }


        private static IndexFieldInspection ExtractFromCtorBody(
            SyntaxNode body,
            SemanticModel model,
            HashSet<string> fields)
        {
            foreach (SyntaxNode node in body.DescendantNodesAndSelf())
            {
                ExpressionSyntax? lambdaBody = null;

                // Map = lambda  /  this.Map = lambda  /  base.Map = lambda
                if (node is AssignmentExpressionSyntax assignment)
                {
                    SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetSimpleMemberName(assignment.Left);
                    if (nameNode != null && nameNode.Identifier.Text == KnownTypes.MapFieldName)
                    {
                        ISymbol? sym = model.GetSymbolInfo(nameNode).Symbol;
                        if (sym is (IFieldSymbol or IPropertySymbol)
                            && SyntaxHelpers.IsDefinedOnIndexBase(sym.ContainingType))
                        {
                            lambdaBody = ExtractLambdaBody(assignment.Right);
                            if (lambdaBody == null)
                                return IndexFieldInspection.BailCannotAnalyze;
                        }
                    }
                }

                // AddMap<T>(..., lambda) or AddMapForAll<T>(..., lambda)
                if (node is InvocationExpressionSyntax invocation)
                {
                    string? methodName = SyntaxHelpers.GetMethodName(invocation);
                    if (methodName == KnownTypes.AddMapMethodName || methodName == KnownTypes.AddMapForAllMethodName)
                    {
                        ISymbol? sym = model.GetSymbolInfo(invocation).Symbol;
                        if (sym is IMethodSymbol method && SyntaxHelpers.IsMultiMapBase(method.ContainingType))
                        {
                            SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
                            if (args.Count == 0)
                                return IndexFieldInspection.BailCannotAnalyze;

                            lambdaBody = ExtractLambdaBody(args[args.Count - 1].Expression);
                            if (lambdaBody == null)
                                return IndexFieldInspection.BailCannotAnalyze;
                        }
                    }
                }

                if (lambdaBody == null)
                    continue;

                ExpressionSyntax? projection = ExtractProjectionExpression(lambdaBody);
                if (projection == null)
                    return IndexFieldInspection.BailCannotAnalyze;

                if (!CollectFieldNames(projection, fields))
                    return IndexFieldInspection.BailCannotAnalyze;
            }

            return IndexFieldInspection.Ok;
        }

        private static ExpressionSyntax? ExtractLambdaBody(ExpressionSyntax expr) =>
            SyntaxHelpers.TryGetLambdaBody(expr);

        /// <summary>
        /// Walks the lambda body (either a query expression or a method chain) to find
        /// the final Select projection expression.
        /// </summary>
        private static ExpressionSyntax? ExtractProjectionExpression(ExpressionSyntax lambdaBody)
        {
            // from x in ... select new { ... }
            if (lambdaBody is QueryExpressionSyntax query)
                return ExtractFromQueryExpression(query);

            // source.Select(x => new { ... })
            if (lambdaBody is InvocationExpressionSyntax)
                return ExtractFromMethodChain(lambdaBody);

            return null;
        }

        private static ExpressionSyntax? ExtractFromQueryExpression(QueryExpressionSyntax query)
        {
            // Walk continuations to get the final select clause
            QueryBodySyntax body = query.Body;
            while (body.Continuation != null)
                body = body.Continuation.Body;

            if (body.SelectOrGroup is SelectClauseSyntax selectClause)
                return selectClause.Expression;

            return null;
        }

        private static ExpressionSyntax? ExtractFromMethodChain(ExpressionSyntax expression)
        {
            // Walk the invocation chain to find the outermost Select or SelectMany
            ExpressionSyntax? selectProjection = null;

            foreach (InvocationExpressionSyntax inv in SyntaxHelpers.EnumerateInvocationChain(expression))
            {
                string? name = SyntaxHelpers.GetMethodName(inv);
                if (name != KnownTypes.SelectMethodName && name != KnownTypes.SelectManyMethodName)
                    continue;

                SeparatedSyntaxList<ArgumentSyntax> args = inv.ArgumentList.Arguments;
                if (args.Count == 0)
                    return null;

                // For SelectMany with a result selector — SelectMany(x => x.Items, (x, i) => new { … }) —
                // the projection is the LAST argument; the first is the collection selector. Plain Select
                // and single-argument SelectMany carry the projection in the first argument.
                int projectionArgIndex = name == KnownTypes.SelectManyMethodName && args.Count >= 2 ? args.Count - 1 : 0;
                ExpressionSyntax? body = ExtractLambdaBody(args[projectionArgIndex].Expression);
                if (body == null)
                    return null;

                selectProjection = body;
                break; // outermost Select is the first one we encounter walking inward
            }

            return selectProjection;
        }

        /// <summary>
        /// Extracts field names from an anonymous object creation or named object initializer.
        /// Returns false if the expression shape cannot be analyzed.
        /// </summary>
        private static bool CollectFieldNames(ExpressionSyntax projection, HashSet<string> fields)
        {
            switch (projection)
            {
                case AnonymousObjectCreationExpressionSyntax anon:
                    foreach (AnonymousObjectMemberDeclaratorSyntax initializer in anon.Initializers)
                    {
                        string? name = GetAnonymousInitializerName(initializer);
                        if (name == null)
                            return false;
                        fields.Add(name);
                    }
                    return true;

                case ObjectCreationExpressionSyntax objectCreation:
                    return CollectFromInitializer(objectCreation.Initializer, fields);

                case ImplicitObjectCreationExpressionSyntax implicitCreation:
                    return CollectFromInitializer(implicitCreation.Initializer, fields);

                default:
                    return false;
            }
        }

        private static bool CollectFromInitializer(InitializerExpressionSyntax? initializer, HashSet<string> fields)
        {
            if (initializer == null
                || !initializer.IsKind(SyntaxKind.ObjectInitializerExpression))
            {
                return false;
            }

            foreach (ExpressionSyntax expr in initializer.Expressions)
            {
                if (expr is not AssignmentExpressionSyntax assignment)
                    return false;
                if (assignment.Left is not IdentifierNameSyntax left)
                    return false;
                fields.Add(left.Identifier.Text);
            }

            return true;
        }

        private static string? GetAnonymousInitializerName(AnonymousObjectMemberDeclaratorSyntax initializer)
        {
            // Explicit name: new { Name = x.SomeProp }
            if (initializer.NameEquals != null)
                return initializer.NameEquals.Name.Identifier.Text;

            // Implicit name inferred from member access: new { x.Name }
            if (initializer.Expression is MemberAccessExpressionSyntax memberAccess)
                return memberAccess.Name.Identifier.Text;

            // Implicit name from simple identifier: new { Name } (rare but valid)
            if (initializer.Expression is IdentifierNameSyntax identifier)
                return identifier.Identifier.Text;

            return null;
        }
    }
}
