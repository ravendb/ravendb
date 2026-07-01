using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Indexes
{
    internal enum StoredFieldsStatus
    {
        Ok,
        AllStored,          // StoreAllFields() was called; treat map-projection fields as stored
        BailCannotAnalyze
    }

    internal record struct IndexStoredFieldSet(StoredFieldsStatus Status, ImmutableHashSet<string> Fields)
    {
        public static readonly IndexStoredFieldSet Bail = new(StoredFieldsStatus.BailCannotAnalyze, ImmutableHashSet<string>.Empty);

        public static readonly IndexStoredFieldSet AllStored = new(StoredFieldsStatus.AllStored, ImmutableHashSet<string>.Empty);
    }

    /// <summary>
    /// Extracts the set of explicitly stored field names from a RavenDB index class constructor.
    /// "Stored" means registered via <c>Store(…)</c>, <c>StoreAllFields(…)</c>,
    /// <c>Stores[…] = FieldStorage.Yes</c>, or <c>StoresStrings[…] = FieldStorage.Yes</c>.
    /// Just being part of the Map projection does NOT make a field stored.
    /// </summary>
    internal static class IndexStoredFieldExtractor
    {
        public static IndexStoredFieldSet Extract(INamedTypeSymbol indexClass, Compilation compilation)
        {
            if (indexClass.DeclaringSyntaxReferences.IsEmpty)
                return IndexStoredFieldSet.Bail;

            if (SyntaxHelpers.IsJavaScriptIndex(indexClass))
                return IndexStoredFieldSet.Bail;

            // Stored-field directives (Store(...) / Stores[...] / StoreAllFields) may live in a shared
            // base index class, so walk the chain (same helper RVN004 and the map-field extractor use).
            // Bail if a base is metadata-only: an unreadable base could store fields we cannot see, which
            // under ProjectionBehavior.FromIndex would make projected fields wrongly look non-retrievable.
            if (!IndexInheritanceInspector.TryCollectChainDeclarations(indexClass, out List<ClassDeclarationSyntax> declarations))
                return IndexStoredFieldSet.Bail;

            var allFields = new HashSet<string>(System.StringComparer.Ordinal);

            foreach (ClassDeclarationSyntax classDecl in declarations)
            {
                SemanticModel model = compilation.GetSemanticModel(classDecl.SyntaxTree);

                foreach (ConstructorDeclarationSyntax ctor in classDecl.Members
                    .OfType<ConstructorDeclarationSyntax>())
                {
                    SyntaxNode? body = ctor.GetBodyNode();
                    if (body == null)
                        continue;

                    StoredFieldsStatus result = ExtractFromCtorBody(body, model, allFields);
                    if (result == StoredFieldsStatus.BailCannotAnalyze)
                        return IndexStoredFieldSet.Bail;
                    if (result == StoredFieldsStatus.AllStored)
                        return IndexStoredFieldSet.AllStored;
                }
            }

            return new IndexStoredFieldSet(StoredFieldsStatus.Ok, allFields.ToImmutableHashSet());
        }

        private static StoredFieldsStatus ExtractFromCtorBody(
            SyntaxNode body,
            SemanticModel model,
            HashSet<string> fields)
        {
            if (SyntaxHelpers.ContainsDynamicFieldCalls(body))
                return StoredFieldsStatus.BailCannotAnalyze;

            foreach (SyntaxNode node in body.DescendantNodesAndSelf())
            {
                // Store(x => x.Field, FieldStorage.Yes) or Store("FieldName", FieldStorage.Yes)
                if (node is InvocationExpressionSyntax invocation)
                {
                    string? methodName = SyntaxHelpers.GetMethodName(invocation);
                    // Also handle plain identifier: Store(...) without this.
                    if (methodName == null && invocation.Expression is IdentifierNameSyntax idName)
                        methodName = idName.Identifier.Text;

                    if (methodName == KnownTypes.StoreAllFieldsMethodName)
                    {
                        // StoreAllFields(FieldStorage.Yes) stores every field; StoreAllFields(No)
                        // stores nothing. Only the explicit Yes form means "all stored"; for No keep
                        // scanning individual Store(...) calls, and bail if the argument can't be read.
                        SeparatedSyntaxList<ArgumentSyntax> storeAllArgs = invocation.ArgumentList.Arguments;
                        if (storeAllArgs.Count == 1 && IsFieldStorageYes(storeAllArgs[0].Expression, model))
                            return StoredFieldsStatus.AllStored;
                        if (storeAllArgs.Count == 1 && IsFieldStorageNo(storeAllArgs[0].Expression, model))
                            continue;
                        return StoredFieldsStatus.BailCannotAnalyze;
                    }

                    if (methodName == KnownTypes.StoreMethodName)
                    {
                        SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
                        if (args.Count < 2)
                            continue;

                        if (!IsFieldStorageYes(args[1].Expression, model))
                            continue;

                        string? fieldName = ExtractFieldNameFromArg(args[0].Expression);
                        if (fieldName == null)
                            return StoredFieldsStatus.BailCannotAnalyze;

                        fields.Add(fieldName);
                        continue;
                    }
                }

                // Stores[x => x.Field] = FieldStorage.Yes  or  StoresStrings["FieldName"] = FieldStorage.Yes
                // The receiver may be bare (Stores[…]) or qualified (this.Stores[…] / base.Stores[…]).
                if (node is AssignmentExpressionSyntax assignment
                    && assignment.Left is ElementAccessExpressionSyntax elementAccess
                    && SyntaxHelpers.TryGetSimpleMemberName(elementAccess.Expression) is SimpleNameSyntax dictNameNode)
                {
                    string dictName = dictNameNode.Identifier.ValueText;
                    if (dictName != KnownTypes.StoresPropertyName && dictName != KnownTypes.StoresStringsPropertyName)
                        continue;

                    SeparatedSyntaxList<ArgumentSyntax> indexerArgs = elementAccess.ArgumentList.Arguments;
                    if (indexerArgs.Count == 0)
                        return StoredFieldsStatus.BailCannotAnalyze;

                    if (!IsFieldStorageYes(assignment.Right, model))
                        continue;

                    string? fieldName = ExtractFieldNameFromArg(indexerArgs[0].Expression);
                    if (fieldName == null)
                        return StoredFieldsStatus.BailCannotAnalyze;

                    fields.Add(fieldName);
                }
            }

            return StoredFieldsStatus.Ok;
        }

        private static bool IsFieldStorageYes(ExpressionSyntax expr, SemanticModel model) =>
            IsFieldStorageMember(expr, KnownTypes.FieldStorageYes, model);

        private static bool IsFieldStorageNo(ExpressionSyntax expr, SemanticModel model) =>
            IsFieldStorageMember(expr, KnownTypes.FieldStorageNo, model);

        // FieldStorage.Yes / FieldStorage.No — a member access whose member name matches AND whose
        // expression type is the Raven.Client FieldStorage enum. The namespace-gated type check keeps
        // an unrelated enum member that merely happens to be named "Yes"/"No" (e.g. MyFlags.Yes) — or a
        // user enum named FieldStorage in another namespace — from being mistaken for a storage
        // directive and corrupting the stored-field set.
        private static bool IsFieldStorageMember(ExpressionSyntax expr, string memberName, SemanticModel model)
        {
            if (expr is not MemberAccessExpressionSyntax ma || ma.Name.Identifier.ValueText != memberName)
                return false;

            ITypeSymbol? type = model.GetTypeInfo(ma).Type;
            return SyntaxHelpers.IsTypeOrImplements(type, KnownTypes.FieldStorageTypeName);
        }

        /// <summary>
        /// Extracts a field name from a Store/Stores argument:
        /// - Lambda form: <c>x => x.FieldName</c> → "FieldName"
        /// - String literal: <c>"FieldName"</c> → "FieldName"
        /// - Anything else: null (bail)
        /// </summary>
        private static string? ExtractFieldNameFromArg(ExpressionSyntax expr)
        {
            // Lambda form: x => x.FieldName  or  (x) => x.FieldName
            if (expr is SimpleLambdaExpressionSyntax simple)
                return ExtractFirstHopName(simple.Parameter.Identifier.ValueText, simple.Body as ExpressionSyntax);

            if (expr is ParenthesizedLambdaExpressionSyntax paren
                && paren.ParameterList.Parameters.Count == 1)
            {
                return ExtractFirstHopName(
                    paren.ParameterList.Parameters[0].Identifier.ValueText,
                    paren.Body as ExpressionSyntax);
            }

            // String literal form
            if (expr is LiteralExpressionSyntax literal
                && literal.IsKind(SyntaxKind.StringLiteralExpression))
            {
                return literal.Token.ValueText;
            }

            return null; // variable or complex expression — bail
        }

        private static string? ExtractFirstHopName(string paramName, ExpressionSyntax? body)
        {
            if (body is MemberAccessExpressionSyntax memberAccess
                && memberAccess.Expression is IdentifierNameSyntax id
                && id.Identifier.ValueText == paramName)
            {
                return memberAccess.Name.Identifier.Text;
            }

            // Cast expression: (object)x.Field
            if (body is CastExpressionSyntax cast)
                return ExtractFirstHopName(paramName, cast.Expression);

            return null;
        }
    }
}
