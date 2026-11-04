using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using DynamicEnumerable = Raven.Server.Documents.Indexes.Static.Linq.DynamicEnumerable;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    // The rewriters leave source choices in calls nested in Count, Distinct or Range arguments, because the right source depends on how the
    // call binds after all rewrites. The generated code is bound once with the sources and references of the index, and each choice is replaced.
    internal static class GeneratedSourceSelector
    {
        // The generated helper calls are qualified with an extern alias, so a type of the index named DynamicEnumerable cannot capture them.
        internal static string QualifyDynamicEnumerable(string alias) => alias + "::" + HelperTypeName;

        private static readonly AssemblyIdentity ServerAssembly = AssemblyIdentity.FromAssemblyDefinition(typeof(DynamicEnumerable).Assembly);

        private static readonly string HelperTypeName = typeof(DynamicEnumerable).FullName!;

        private static readonly string RangeSourceTypeName = typeof(DynamicEnumerable.RangeSource).FullName!;

        private static readonly string[] RangeSourceChoice = ["DynamicEnumerable", "RangeSource", "Instance", "ChooseRangeSource"];

        private static readonly string[] ObjectDelegateSource = ["DynamicEnumerable", "ObjectDelegateSource"];

        private static readonly string[] DistinctFromEnumerable = ["DynamicEnumerable", nameof(DynamicEnumerable.DistinctFromEnumerable)];

        private static readonly string[] DistinctFromEnumerableComparerFirst = ["DynamicEnumerable", nameof(DynamicEnumerable.DistinctFromEnumerableComparerFirst)];

        public static bool TrySelect(SyntaxNode compilationUnit, IReadOnlyCollection<SyntaxTree> additionalSyntaxTrees, IEnumerable<MetadataReference> references, string dynamicEnumerable,
            out SyntaxNode selected)
        {
            selected = null;
            if (compilationUnit.DescendantNodes().OfType<InvocationExpressionSyntax>().Any(IsChoice) == false)
                return false;

            var tree = SyntaxFactory.ParseSyntaxTree(compilationUnit.ToFullString());
            var compilation = CSharpCompilation.Create(
                assemblyName: string.Empty,
                syntaxTrees: additionalSyntaxTrees.Append(tree),
                references: references,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
            var semanticModel = compilation.GetSemanticModel(tree);

            // The index may call the helpers itself, so a choice must bind to the server's helper and, for Distinct and markers, carry the alias.
            var server = compilation.SourceModule.ReferencedAssemblySymbols.FirstOrDefault(assembly => assembly.Identity.Equals(ServerAssembly));
            var helperType = server?.GetTypeByMetadataName(HelperTypeName);
            var rangeSource = server?.GetTypeByMetadataName(RangeSourceTypeName);
            var equalityComparer = GetEqualityComparer(helperType);
            var root = tree.GetRoot();
            var helperCalls = root.DescendantNodes().OfType<InvocationExpressionSyntax>()
                .Where(invocation => IsChoice(invocation) && IsHelperCall(semanticModel, invocation, helperType, rangeSource))
                .ToList();
            var generatedReceiver = SyntaxFactory.ParseExpression(dynamicEnumerable);
            var generated = helperCalls
                .Where(invocation => IsRangeSourceChoice(invocation) == false && ((MemberAccessExpressionSyntax)invocation.Expression).Expression.IsEquivalentTo(generatedReceiver))
                .ToHashSet();
            var choices = new List<InvocationExpressionSyntax>();
            var consumers = new Dictionary<InvocationExpressionSyntax, SyntaxNode>();
            foreach (var invocation in helperCalls)
            {
                if (IsDistinct(invocation) == false)
                {
                    if (IsRangeSourceChoice(invocation) || generated.Contains(invocation))
                        choices.Add(invocation);

                    continue;
                }

                var demand = generated.Contains(invocation) ? SelectsDynamicResult(semanticModel, generated, equalityComparer, invocation) : Demand.None;
                if (demand == Demand.None)
                    continue;

                choices.Add(invocation);
                if (demand == Demand.Checked)
                    consumers.Add(invocation, (SyntaxNode)GetConsumerArgument(generated, invocation) ?? invocation);
            }

            if (choices.Count == 0)
                return false;

            var objectElements = new HashSet<InvocationExpressionSyntax>();
            foreach (var choice in choices)
            {
                if (IsDistinct(choice))
                    continue;

                if (IsRangeSourceChoice(choice) ? SelectsObjectElements(semanticModel, choice) : NeedsObjectElements(semanticModel, choice))
                    objectElements.Add(choice);
            }

            var groups = consumers.Values.Distinct().ToDictionary(x => x, _ => new SyntaxAnnotation(CheckedResult));
            var annotations = consumers.ToDictionary(x => x.Key, x => groups[x.Value]);
            var guards = new Dictionary<InvocationExpressionSyntax, (List<TypeSyntax> Views, string Name)>();
            HashSet<string> names = null;
            foreach (var (distinct, consumer) in consumers)
            {
                if (GetViews(semanticModel, distinct, consumer) is { } views)
                {
                    names ??= GetNames(additionalSyntaxTrees.Append(tree));
                    guards.Add(distinct, (views, GetFreshName(names, "__distinctResult")));
                }
            }

            var remaining = groups.Values.ToHashSet();
            selected = ReplaceChoices(root, choices, objectElements, annotations, guards, dynamicEnumerable);

            // A consumer that does not bind with the dynamic results did not compile as written either: its checked results are left as they were
            // and the code is bound again.
            while (remaining.Count > 0 && selected != null)
            {
                var selectedTree = tree.WithRootAndOptions(selected, tree.Options);
                var selectedModel = compilation.ReplaceSyntaxTree(tree, selectedTree).GetSemanticModel(selectedTree);
                if (GetFailedResults(selectedModel, remaining) is not { } failed)
                    break;

                remaining.ExceptWith(failed);
                choices.RemoveAll(x => annotations.TryGetValue(x, out var annotation) && failed.Contains(annotation));
                selected = choices.Count > 0 ? ReplaceChoices(root, choices, objectElements, annotations, guards, dynamicEnumerable) : null;
            }

            return selected != null;
        }

        private static HashSet<string> GetNames(IEnumerable<SyntaxTree> trees)
        {
            return trees.SelectMany(tree => tree.GetRoot().DescendantTokens())
                .Where(token => token.IsKind(SyntaxKind.IdentifierToken))
                .Select(token => token.ValueText)
                .ToHashSet();
        }

        private static string GetFreshName(HashSet<string> names, string name)
        {
            var fresh = name;
            for (var suffix = 1; names.Add(fresh) == false; suffix++)
                fresh = name + "_" + suffix;

            return fresh;
        }

        private const string CheckedResult = nameof(CheckedResult);

        private static SyntaxNode ReplaceChoices(SyntaxNode root, List<InvocationExpressionSyntax> choices, HashSet<InvocationExpressionSyntax> objectElements,
            Dictionary<InvocationExpressionSyntax, SyntaxAnnotation> annotations, Dictionary<InvocationExpressionSyntax, (List<TypeSyntax> Views, string Name)> guards,
            string dynamicEnumerable)
        {
            return root.ReplaceNodes(choices, (choice, rewritten) =>
            {
                if (IsDistinct(choice))
                {
                    var dynamicResult = SelectDynamicResult((InvocationExpressionSyntax)rewritten);
                    if (annotations.TryGetValue(choice, out var annotation) == false)
                        return dynamicResult;

                    return guards.TryGetValue(choice, out var guard)
                        ? GuardDynamicResult(dynamicResult, guard.Views, guard.Name, dynamicEnumerable).WithAdditionalAnnotations(annotation)
                        : dynamicResult.WithAdditionalAnnotations(annotation);
                }

                return IsRangeSourceChoice(choice)
                    ? SelectRangeSource((InvocationExpressionSyntax)rewritten, objectElements.Contains(choice))
                    : SelectObjectDelegateSource((InvocationExpressionSyntax)rewritten, objectElements.Contains(choice), dynamicEnumerable);
            });
        }

        // An error outside every checked result cannot be attributed to one, so all remaining results are given up then.
        private static HashSet<SyntaxAnnotation> GetFailedResults(SemanticModel selectedModel, HashSet<SyntaxAnnotation> remaining)
        {
            var errors = selectedModel.GetDiagnostics()
                .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
                .ToList();
            if (errors.Count == 0)
                return null;

            var selectedTree = selectedModel.SyntaxTree;
            var selectedRoot = selectedTree.GetRoot();
            var failed = new HashSet<SyntaxAnnotation>();
            foreach (var error in errors)
            {
                if (error.Location.SourceTree != selectedTree)
                    continue;

                var results = selectedRoot.FindNode(error.Location.SourceSpan, getInnermostNodeForTie: true).AncestorsAndSelf()
                    .Select(node => node.GetAnnotatedNodes(CheckedResult).ToList())
                    .FirstOrDefault(nodes => nodes.Count > 0);
                foreach (var result in results ?? Enumerable.Empty<SyntaxNode>())
                    failed.UnionWith(result.GetAnnotations(CheckedResult));
            }

            failed.IntersectWith(remaining);
            return failed.Count > 0 ? failed : remaining.ToHashSet();
        }

        private static bool IsChoice(InvocationExpressionSyntax invocation) =>
            IsRangeSourceChoice(invocation) || IsAccess(invocation.Expression, ObjectDelegateSource) || IsDistinct(invocation);

        private static bool IsDistinct(InvocationExpressionSyntax invocation) =>
            IsAccess(invocation.Expression, DistinctFromEnumerable) || IsAccess(invocation.Expression, DistinctFromEnumerableComparerFirst);

        internal static bool IsRangeSourceChoice(InvocationExpressionSyntax invocation) => IsAccess(invocation.Expression, RangeSourceChoice);

        // The receiver is checked instead of the call, which binds dynamically for a dynamic argument.
        private static bool IsHelperCall(SemanticModel semanticModel, InvocationExpressionSyntax invocation, INamedTypeSymbol dynamicEnumerable, INamedTypeSymbol rangeSource)
        {
            if (invocation.ArgumentList.Arguments is not [{ NameColon: null } argument])
                return false;

            var receiver = semanticModel.GetSymbolInfo(((MemberAccessExpressionSyntax)invocation.Expression).Expression).Symbol;
            return IsRangeSourceChoice(invocation)
                ? argument.Expression is SimpleLambdaExpressionSyntax consumer && GetRangeSource(consumer) != null &&
                  receiver is IFieldSymbol field && SymbolEqualityComparer.Default.Equals(field.ContainingType, rangeSource)
                : receiver is INamedTypeSymbol type && SymbolEqualityComparer.Default.Equals(type, dynamicEnumerable);
        }

        private static bool IsAccess(ExpressionSyntax expression, string[] names)
        {
            for (var i = names.Length - 1; i > 0; i--)
            {
                if (expression is not MemberAccessExpressionSyntax memberAccess || memberAccess.Name.Identifier.ValueText != names[i])
                    return false;

                expression = memberAccess.Expression;
            }

            return expression switch
            {
                IdentifierNameSyntax identifier => identifier.Identifier.ValueText == names[0],
                MemberAccessExpressionSyntax memberAccess => memberAccess.Name.Identifier.ValueText == names[0],
                _ => false
            };
        }

        private static InvocationExpressionSyntax GetRangeSource(SimpleLambdaExpressionSyntax consumer)
        {
            if (consumer.ExpressionBody == null)
                return null;

            var parameter = consumer.Parameter.Identifier.ValueText;
            var sources = consumer.ExpressionBody.DescendantNodes().OfType<InvocationExpressionSyntax>()
                .Where(invocation => invocation.Expression is IdentifierNameSyntax identifier && identifier.Identifier.ValueText == parameter)
                .Take(2)
                .ToList();
            return sources is [{ ArgumentList.Arguments: [{ NameColon: null }] } source] ? source : null;
        }

        private static bool SelectsObjectElements(SemanticModel semanticModel, InvocationExpressionSyntax choice)
        {
            return semanticModel.GetSymbolInfo(choice).Symbol is IMethodSymbol { ReducedFrom: not null };
        }

        private static ExpressionSyntax SelectRangeSource(InvocationExpressionSyntax choice, bool objectElements)
        {
            var consumer = (SimpleLambdaExpressionSyntax)choice.ArgumentList.Arguments[0].Expression;
            var body = consumer.ExpressionBody!;
            var source = GetRangeSource(consumer)!;
            var range = source.ArgumentList.Arguments[0].Expression;

            var selectedSource = objectElements
                ? SyntaxFactory.ParseExpression($"Enumerable.Cast<dynamic>({range})")
                : SyntaxFactory.ParenthesizedExpression(range);
            return SyntaxFactory.ParenthesizedExpression(body.ReplaceNode(source, selectedSource.WithTriviaFrom(source)));
        }

        private static bool NeedsObjectElements(SemanticModel semanticModel, InvocationExpressionSyntax marker)
        {
            var source = marker.ArgumentList.Arguments[0].Expression;
            if (semanticModel.GetTypeInfo(source).Type is not { TypeKind: TypeKind.Dynamic })
                return false;

            if (marker.Parent is not ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax consumer } })
                return false;

            var objectElementDelegate = false;
            foreach (var argument in consumer.ArgumentList.Arguments)
            {
                if (argument.Expression == marker ||
                    semanticModel.GetTypeInfo(argument.Expression).Type is not INamedTypeSymbol { TypeKind: TypeKind.Delegate, DelegateInvokeMethod: { } invoke } ||
                    invoke.Parameters.Length == 0)
                    continue;

                var element = invoke.Parameters[0].Type;
                if (element.SpecialType != SpecialType.System_Object && element.TypeKind != TypeKind.Dynamic)
                    return false;

                objectElementDelegate = true;
            }

            return objectElementDelegate;
        }

        private static ExpressionSyntax SelectObjectDelegateSource(InvocationExpressionSyntax marker, bool objectElements, string dynamicEnumerable)
        {
            var source = marker.ArgumentList.Arguments[0].Expression;
            return objectElements
                ? SyntaxFactory.ParseExpression($"{dynamicEnumerable}.PrepareSourceForObjectDelegate((object)({source}))").WithTriviaFrom(marker)
                : SyntaxFactory.ParenthesizedExpression(source).WithTriviaFrom(marker);
        }

        private enum Demand
        {
            None,
            // The dynamic result is kept only if the selected code binds.
            Checked,
            Certain
        }

        private static Demand SelectsDynamicResult(SemanticModel semanticModel, HashSet<InvocationExpressionSyntax> generated, INamedTypeSymbol equalityComparer,
            InvocationExpressionSyntax distinct)
        {
            var demand = GetDemand(semanticModel, generated, equalityComparer, distinct);
            if (demand == Demand.None || semanticModel.GetSymbolInfo(distinct).Symbol is IMethodSymbol { IsGenericMethod: false })
                return demand;

            if (TryGetOperands(distinct, out var source, out _) == false)
                return Demand.None;

            while (source is ParenthesizedExpressionSyntax parenthesized)
                source = parenthesized.Expression;

            return source is InvocationExpressionSyntax inner && IsDistinct(inner) && generated.Contains(inner) &&
                   SelectsDynamicResult(semanticModel, generated, equalityComparer, inner) != Demand.None
                ? demand
                : Demand.None;
        }

        private static Demand GetDemand(SemanticModel semanticModel, HashSet<InvocationExpressionSyntax> generated, INamedTypeSymbol equalityComparer, ExpressionSyntax expression)
        {
            var result = expression;
            while (result.Parent is ParenthesizedExpressionSyntax parenthesized)
                result = parenthesized;

            switch (result.Parent)
            {
                case CastExpressionSyntax cast:
                    return IsTypedSequence(semanticModel.GetTypeInfo(cast.Type).Type) ? Demand.Certain : Demand.None;
                case BinaryExpressionSyntax binary when binary.IsKind(SyntaxKind.IsExpression) || binary.IsKind(SyntaxKind.AsExpression):
                    return binary.Left == result ? Demand.Certain : Demand.None;
                case IsPatternExpressionSyntax isPattern when isPattern.Expression == result:
                    return IsTypeTest(isPattern.Pattern) ? Demand.Certain : Demand.None;
                case SwitchExpressionSyntax switchExpression when switchExpression.GoverningExpression == result:
                    return switchExpression.Arms.All(arm => IsEligiblePattern(arm.Pattern)) && switchExpression.Arms.Any(arm => IsTypeTest(arm.Pattern))
                        ? Demand.Certain
                        : Demand.None;
                // The compiler resolves the conversion to the declared type, also when the object result does not convert to it.
                case EqualsValueClauseSyntax { Parent: VariableDeclaratorSyntax }:
                case AssignmentExpressionSyntax assignment when assignment.IsKind(SyntaxKind.SimpleAssignmentExpression) && assignment.Right == result:
                case ReturnStatementSyntax:
                case LambdaExpressionSyntax lambda when lambda.ExpressionBody == result:
                    return IsTypedSequence(semanticModel.GetTypeInfo(result).ConvertedType) ? Demand.Certain : Demand.None;
                case ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax marker } }
                    when IsAccess(marker.Expression, ObjectDelegateSource) && generated.Contains(marker):
                    return GetDemand(semanticModel, generated, equalityComparer, marker);
                case ArgumentSyntax { Parent: ArgumentListSyntax { Parent: ExpressionSyntax consumer and (InvocationExpressionSyntax or BaseObjectCreationExpressionSyntax) } argumentList } argument:
                    return GetArgumentDemand(semanticModel, consumer, argumentList, argument);
                case ArgumentSyntax { Parent: TupleExpressionSyntax { Parent: ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax distinct } } } }
                    when IsDistinct(distinct) && generated.Contains(distinct):
                    if (TryGetOperands(distinct, out var source, out var comparer) == false || source != result)
                        return Demand.None;

                    return IsTypedComparer(semanticModel, equalityComparer, comparer) ? Demand.Certain : GetDemand(semanticModel, generated, equalityComparer, distinct);
                default:
                    return Demand.None;
            }
        }

        private static bool IsTypeTest(PatternSyntax pattern)
        {
            return IsEligiblePattern(pattern) &&
                   pattern.DescendantNodesAndSelf().Any(node => node is TypePatternSyntax or DeclarationPatternSyntax or RecursivePatternSyntax { Type: not null });
        }

        // A pattern whose bindings do not depend on the static type of the tested value. A var pattern or a recursive pattern without a type
        // binds to that type, so the object result is kept for it; a recursive pattern with a type binds its subpatterns to that type.
        private static bool IsEligiblePattern(PatternSyntax pattern)
        {
            return pattern switch
            {
                ParenthesizedPatternSyntax parenthesized => IsEligiblePattern(parenthesized.Pattern),
                UnaryPatternSyntax unary when unary.OperatorToken.IsKind(SyntaxKind.NotKeyword) => IsEligiblePattern(unary.Pattern),
                BinaryPatternSyntax binary => IsEligiblePattern(binary.Left) && IsEligiblePattern(binary.Right),
                TypePatternSyntax or DeclarationPatternSyntax or RecursivePatternSyntax { Type: not null } or DiscardPatternSyntax => true,
                ConstantPatternSyntax { Expression: LiteralExpressionSyntax literal } => literal.IsKind(SyntaxKind.NullLiteralExpression),
                _ => false
            };
        }

        private static ArgumentSyntax GetConsumerArgument(HashSet<InvocationExpressionSyntax> generated, ExpressionSyntax expression)
        {
            while (true)
            {
                var result = expression;
                while (result.Parent is ParenthesizedExpressionSyntax parenthesized)
                    result = parenthesized;

                switch (result.Parent)
                {
                    case ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax marker } }
                        when IsAccess(marker.Expression, ObjectDelegateSource) && generated.Contains(marker):
                        expression = marker;
                        break;
                    case ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax or BaseObjectCreationExpressionSyntax } } argument:
                        return argument;
                    case ArgumentSyntax { Parent: TupleExpressionSyntax { Parent: ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax distinct } } } }
                        when IsDistinct(distinct) && generated.Contains(distinct):
                        expression = distinct;
                        break;
                    default:
                        return null;
                }
            }
        }

        // The call in the index chose its overload at runtime, so every candidate with a typed sequence parameter counts, also when the call binds.
        private static Demand GetArgumentDemand(SemanticModel semanticModel, ExpressionSyntax consumer, ArgumentListSyntax argumentList, ArgumentSyntax argument)
        {
            var consumerInfo = semanticModel.GetSymbolInfo(consumer);
            var index = argumentList.Arguments.IndexOf(argument);
            var typedCandidate = GetCandidates(semanticModel, consumer, consumerInfo)
                .Any(method => new[] { false, true }.Any(expanded => GetParameterTypes(method, argumentList, expanded) is { } types && IsTypedSequence(types[index])));
            if (typedCandidate == false)
                return Demand.None;

            // A call that did not bind with the object result did not compile; one that binds, also dynamically, is checked.
            return consumerInfo.Symbol == null && consumerInfo.CandidateReason != CandidateReason.LateBound ? Demand.Certain : Demand.Checked;
        }

        private static IEnumerable<IMethodSymbol> GetCandidates(SemanticModel semanticModel, ExpressionSyntax consumer, SymbolInfo consumerInfo)
        {
            IEnumerable<ISymbol> methods = ImmutableArray<ISymbol>.Empty;
            if (consumer is InvocationExpressionSyntax invocation)
                methods = semanticModel.GetMemberGroup(invocation.Expression);
            else if (semanticModel.GetTypeInfo(consumer).Type is INamedTypeSymbol type)
                methods = type.InstanceConstructors;

            return methods.Concat(consumerInfo.CandidateSymbols).Append(consumerInfo.Symbol)
                .OfType<IMethodSymbol>()
                .Where(method => semanticModel.IsAccessible(consumer.SpanStart, method));
        }

        // A consumer with overloads for an object sequence and for typed sequences dispatches at runtime, where a result of a value element type
        // takes the typed overload. A result that matches none of the typed parameters is passed as objects, so the object sequence overload
        // binds as written for it; the binder keeps an ambiguity among typed overloads as before.
        private static List<TypeSyntax> GetViews(SemanticModel semanticModel, InvocationExpressionSyntax distinct, SyntaxNode consumer)
        {
            if (consumer is not ArgumentSyntax { Parent: ArgumentListSyntax { Parent: ExpressionSyntax call and (InvocationExpressionSyntax or BaseObjectCreationExpressionSyntax) } argumentList } argument)
                return null;

            var expression = argument.Expression;
            while (expression is ParenthesizedExpressionSyntax parenthesized)
                expression = parenthesized.Expression;

            if (expression != distinct)
                return null;

            var index = argumentList.Arguments.IndexOf(argument);
            var views = new List<ITypeSymbol>();
            var objectSequence = false;
            foreach (var method in GetCandidates(semanticModel, call, semanticModel.GetSymbolInfo(call)))
            {
                foreach (var expanded in new[] { false, true })
                {
                    if (GetParameterTypes(method, argumentList, expanded) is not { } types)
                        continue;

                    var type = types[index];
                    // Use the expanded object-sequence view for this positional params argument. A normal-form array
                    // keeps its identity through the object facade.
                    if (expanded == false && argument.NameColon == null && method.Parameters is [.., { IsParams: true }] &&
                        type is IArrayTypeSymbol { IsSZArray: true, ElementType: INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T, TypeArguments: [var paramsElement] } paramsSequence } &&
                        (paramsElement.SpecialType == SpecialType.System_Object || paramsElement.TypeKind == TypeKind.Dynamic) &&
                        GetParameterTypes(method, argumentList, true) is { } expandedTypes && SymbolEqualityComparer.Default.Equals(expandedTypes[index], paramsSequence))
                        continue;

                    if (IsTypedSequence(type))
                    {
                        if (views.Any(view => SymbolEqualityComparer.Default.Equals(view, type)) == false)
                            views.Add(type);
                    }
                    else if (type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T, TypeArguments: [var element] } &&
                             (element.SpecialType == SpecialType.System_Object || element.TypeKind == TypeKind.Dynamic))
                        objectSequence = true;
                    else
                        return null;
                }
            }

            if (objectSequence == false || views.Count == 0)
                return null;

            // The qualified name of a referenced type may also be the name of a type of the index sources, so it must bind to the type.
            var position = call.SpanStart;
            if (semanticModel.GetSpeculativeTypeInfo(position, SyntaxFactory.IdentifierName("dynamic"), SpeculativeBindingOption.BindAsTypeOrNamespace).Type is not { TypeKind: TypeKind.Dynamic })
                return null;

            var syntaxes = new List<TypeSyntax>();
            foreach (var view in views)
            {
                var syntax = SyntaxFactory.ParseTypeName(view.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat));
                if (SymbolEqualityComparer.Default.Equals(semanticModel.GetSpeculativeTypeInfo(position, syntax, SpeculativeBindingOption.BindAsTypeOrNamespace).Type, view) == false)
                    return null;

                syntaxes.Add(syntax);
            }

            return syntaxes;
        }

        // The switch is built from compiler-owned nodes with a placeholder, so no name written in the index is taken for a generated one.
        private static ExpressionSyntax GuardDynamicResult(InvocationExpressionSyntax dynamicResult, List<TypeSyntax> views, string name, string dynamicEnumerable)
        {
            var result = SyntaxFactory.IdentifierName(name);
            var dynamicType = SyntaxFactory.IdentifierName("dynamic");
            var facade = SyntaxFactory.ParseExpression($"{dynamicEnumerable}.{nameof(DynamicEnumerable.DistinctResultAsObjects)}");
            var matches = views
                .Select(view => (ExpressionSyntax)SyntaxFactory.BinaryExpression(SyntaxKind.IsExpression, result, view))
                .Aggregate((left, right) => SyntaxFactory.BinaryExpression(SyntaxKind.LogicalOrExpression, left, right));
            var template = SyntaxFactory.SwitchExpression(SyntaxFactory.LiteralExpression(SyntaxKind.DefaultLiteralExpression), SyntaxFactory.SeparatedList(new[]
                {
                    SyntaxFactory.SwitchExpressionArm(Pattern(), SyntaxFactory.WhenClause(matches), SyntaxFactory.CastExpression(dynamicType, result)),
                    SyntaxFactory.SwitchExpressionArm(Pattern(), SyntaxFactory.CastExpression(dynamicType, SyntaxFactory.InvocationExpression(facade,
                        SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(SyntaxFactory.Argument(result))))))
                }))
                .NormalizeWhitespace();

            var objectType = SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.ObjectKeyword));
            var governing = SyntaxFactory.ParenthesizedExpression(SyntaxFactory.CastExpression(objectType, dynamicResult.WithoutTrivia()));
            return SyntaxFactory.ParenthesizedExpression(template.ReplaceNode(template.GoverningExpression, governing)).WithTriviaFrom(dynamicResult);

            PatternSyntax Pattern() => SyntaxFactory.VarPattern(SyntaxFactory.SingleVariableDesignation(SyntaxFactory.Identifier(name)));
        }

        private static ITypeSymbol[] GetParameterTypes(IMethodSymbol method, ArgumentListSyntax argumentList, bool expanded)
        {
            var parameters = method.Parameters;
            var last = parameters.Length - 1;
            if (expanded && (last == -1 || parameters[last] is not { IsParams: true, Type: IArrayTypeSymbol }))
                return null;

            var supplied = new bool[parameters.Length];
            var types = new ITypeSymbol[argumentList.Arguments.Count];
            for (var i = 0; i < types.Length; i++)
            {
                var current = argumentList.Arguments[i];
                var index = current.NameColon != null
                    ? parameters.IndexOf(parameters.FirstOrDefault(x => x.Name == current.NameColon.Name.Identifier.ValueText))
                    : expanded && i >= last
                        ? last
                        : i < parameters.Length ? i : -1;
                var element = expanded && index == last;
                if (index == -1 || supplied[index] && (element == false || current.NameColon != null))
                    return null;

                supplied[index] = true;
                types[i] = element ? ((IArrayTypeSymbol)parameters[index].Type).ElementType : parameters[index].Type;
            }

            return parameters.Where((x, i) => supplied[i] == false && x.IsOptional == false && (expanded == false || i != last)).Any() ? null : types;
        }

        private static bool TryGetOperands(InvocationExpressionSyntax distinct, out ExpressionSyntax source, out ExpressionSyntax comparer)
        {
            source = null;
            comparer = null;
            if (distinct.ArgumentList.Arguments is not [{ Expression: TupleExpressionSyntax { Arguments: [var first, var second] } }])
                return false;

            var comparerFirst = IsAccess(distinct.Expression, DistinctFromEnumerableComparerFirst);
            source = (comparerFirst ? second : first).Expression;
            comparer = (comparerFirst ? first : second).Expression;
            return true;
        }

        private static bool IsTypedComparer(SemanticModel semanticModel, INamedTypeSymbol equalityComparer, ExpressionSyntax comparer)
        {
            var type = semanticModel.GetTypeInfo(comparer).Type;
            if (type is { TypeKind: TypeKind.Dynamic })
                return true;

            var compilation = semanticModel.Compilation;
            return type != null && equalityComparer != null &&
                   (SymbolEqualityComparer.Default.Equals(type.OriginalDefinition, equalityComparer) ||
                    type.AllInterfaces.Any(x => SymbolEqualityComparer.Default.Equals(x.OriginalDefinition, equalityComparer))) &&
                   compilation.ClassifyCommonConversion(type, equalityComparer.Construct(compilation.ObjectType)).IsImplicit == false;
        }

        // The comparer type of the typed Distinct helper, which is the framework type also when a type of the index sources has its name.
        private static INamedTypeSymbol GetEqualityComparer(INamedTypeSymbol helperType)
        {
            return helperType?.GetMembers(nameof(DynamicEnumerable.DistinctFromEnumerable)).OfType<IMethodSymbol>()
                .Select(method => method is { IsGenericMethod: true, Parameters: [{ Type: INamedTypeSymbol { IsTupleType: true, TupleElements: [_, var comparer] } }] }
                    ? comparer.Type.OriginalDefinition as INamedTypeSymbol
                    : null)
                .FirstOrDefault(type => type != null);
        }

        private static bool IsTypedSequence(ITypeSymbol type)
        {
            return type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T, TypeArguments: [var element] } &&
                   element.SpecialType != SpecialType.System_Object &&
                   element.TypeKind is not (TypeKind.Dynamic or TypeKind.TypeParameter or TypeKind.Error);
        }

        private static InvocationExpressionSyntax SelectDynamicResult(InvocationExpressionSyntax distinct)
        {
            var memberAccess = (MemberAccessExpressionSyntax)distinct.Expression;
            var name = memberAccess.Name.Identifier;
            var dynamicResult = name.ValueText == nameof(DynamicEnumerable.DistinctFromEnumerable)
                ? nameof(DynamicEnumerable.DynamicDistinctFromEnumerable)
                : nameof(DynamicEnumerable.DynamicDistinctFromEnumerableComparerFirst);
            return distinct.WithExpression(memberAccess.WithName(SyntaxFactory.IdentifierName(SyntaxFactory.Identifier(name.LeadingTrivia, dynamicResult, name.TrailingTrivia))));
        }
    }
}
