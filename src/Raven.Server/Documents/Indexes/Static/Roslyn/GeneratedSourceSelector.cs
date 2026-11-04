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
            var plans = new Dictionary<ExpressionSyntax, ConsumerPlan>();
            HashSet<string> names = null;
            foreach (var results in groups.Where(x => x.Key is ArgumentSyntax).GroupBy(x => (ExpressionSyntax)x.Key.Parent!.Parent!, x => ((ArgumentSyntax)x.Key, x.Value)))
            {
                names ??= GetNames(additionalSyntaxTrees.Append(tree));
                if (GetPlan(semanticModel, results.Key, results.ToList(), plans.Count, names) is { } plan)
                    plans.Add(plan.Consumer, plan);
            }

            var remaining = groups.Values.ToHashSet();
            selected = ReplaceChoices(root, choices, objectElements, annotations, plans, dynamicEnumerable);

            // A consumer that does not bind with the dynamic results did not compile as written either: its checked results are left as they were
            // and the code is bound again. A consumer whose selected calls bind to other members than planned keeps the dynamic call.
            while (remaining.Count > 0 && selected != null)
            {
                var selectedTree = tree.WithRootAndOptions(selected, tree.Options);
                var selectedModel = compilation.ReplaceSyntaxTree(tree, selectedTree).GetSemanticModel(selectedTree);
                if (GetFailedResults(selectedModel, remaining) is { } failed)
                {
                    remaining.ExceptWith(failed);
                    choices.RemoveAll(x => annotations.TryGetValue(x, out var annotation) && failed.Contains(annotation));
                    foreach (var plan in plans.Values.Where(x => x.Annotations.Overlaps(failed)).ToList())
                        plans.Remove(plan.Consumer);
                }
                else if (plans.Values.Where(x => BindsPlannedMembers(selectedModel, x) == false).ToList() is { Count: > 0 } unplanned)
                {
                    foreach (var plan in unplanned)
                        plans.Remove(plan.Consumer);
                }
                else
                    break;

                selected = choices.Count > 0 ? ReplaceChoices(root, choices, objectElements, annotations, plans, dynamicEnumerable) : null;
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
            Dictionary<InvocationExpressionSyntax, SyntaxAnnotation> annotations, Dictionary<ExpressionSyntax, ConsumerPlan> plans, string dynamicEnumerable)
        {
            return root.ReplaceNodes(choices.Concat(plans.Keys), (original, rewritten) =>
            {
                if (plans.TryGetValue(original, out var plan))
                    return SelectView(rewritten, plan, dynamicEnumerable);

                var choice = (InvocationExpressionSyntax)original;
                if (IsDistinct(choice))
                {
                    var dynamicResult = SelectDynamicResult((InvocationExpressionSyntax)rewritten);
                    return annotations.TryGetValue(choice, out var annotation) ? dynamicResult.WithAdditionalAnnotations(annotation) : dynamicResult;
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

        // Symbols of the two compilations are compared by assembly and documentation id; a late-bound call is decided at runtime.
        private static bool BindsPlannedMembers(SemanticModel selectedModel, ConsumerPlan plan)
        {
            var selectedRoot = selectedModel.SyntaxTree.GetRoot();
            foreach (var (annotation, members) in plan.Calls)
            {
                foreach (var call in selectedRoot.GetAnnotatedNodes(annotation))
                {
                    var symbolInfo = selectedModel.GetSymbolInfo(call);
                    if (symbolInfo.Symbol == null
                            ? symbolInfo.CandidateReason != CandidateReason.LateBound
                            : members != null && members.Contains(GetMemberId(symbolInfo.Symbol)) == false)
                        return false;
                }
            }

            return true;
        }

        private static string GetMemberId(ISymbol symbol) => symbol.ContainingAssembly?.Identity.GetDisplayName() + "|" + symbol.OriginalDefinition.GetDocumentationCommentId();

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

        private sealed class ConsumerPlan
        {
            public ExpressionSyntax Consumer;
            public HashSet<SyntaxAnnotation> Annotations;
            // The receiver (no argument) and the non-constant arguments in written order, each with its variable; a result also has its view index.
            public List<(int Argument, int View, string Name)> Elements;
            public List<Arm> Arms;
            public TypeSyntax Dynamic;
            public SyntaxAnnotation Facade;
            // The results match no view whatever their sources return, so the object results are passed in the call as written.
            public bool Written;
            public List<(SyntaxAnnotation Call, HashSet<string> Members)> Calls;
        }

        private sealed class Arm
        {
            public List<ITypeSymbol> Views;
            public List<TypeSyntax> Types;
            // The views of each other overload the results may match as well, which they must not match all for the arm to apply.
            public List<List<(int View, ITypeSymbol Symbol, TypeSyntax Type)>> Excluded;
            public SyntaxAnnotation Call;
        }

        // A consumer with overloads for sequences of incomparable reference element types, e.g. of two interfaces, cannot be dispatched at
        // runtime for results that implement both. So the results are tested for the views of one overload and the call is bound statically
        // with them when no other overload would win; otherwise the consumer is called as written with the object results.
        private static ConsumerPlan GetPlan(SemanticModel semanticModel, ExpressionSyntax consumer, List<(ArgumentSyntax Argument, SyntaxAnnotation Annotation)> results, int id,
            HashSet<string> names)
        {
            if (consumer is not (InvocationExpressionSyntax or ObjectCreationExpressionSyntax) || consumer.Parent is ExpressionStatementSyntax)
                return null;

            var argumentList = consumer is InvocationExpressionSyntax consumerCall ? consumerCall.ArgumentList : ((ObjectCreationExpressionSyntax)consumer).ArgumentList!;
            var arguments = argumentList.Arguments;
            var sources = results.Where(x => Unwrap(x.Argument.Expression) is InvocationExpressionSyntax distinct && IsDistinct(distinct))
                .Select(x => arguments.IndexOf(x.Argument))
                .OrderBy(x => x)
                .ToList();
            if (sources.Count == 0)
                return null;

            var dynamicArguments = results.Select(x => arguments.IndexOf(x.Argument)).Except(sources).ToHashSet();
            var capturesReceiver = false;
            if (consumer is InvocationExpressionSyntax invocation)
            {
                if (invocation.Expression is MemberAccessExpressionSyntax memberAccess && memberAccess.IsKind(SyntaxKind.SimpleMemberAccessExpression))
                {
                    if (semanticModel.GetSymbolInfo(memberAccess.Expression).Symbol is not (ITypeSymbol or INamespaceSymbol))
                    {
                        if (memberAccess.Expression is BaseExpressionSyntax ||
                            semanticModel.GetTypeInfo(memberAccess.Expression).Type is not { IsReferenceType: true, TypeKind: not TypeKind.Dynamic })
                            return null;

                        capturesReceiver = true;
                    }
                }
                else if (invocation.Expression is not SimpleNameSyntax)
                    return null;
            }

            var elements = new List<(int Argument, int View, string Name)>();
            if (capturesReceiver)
                elements.Add((-1, -1, null));

            for (var i = 0; i < arguments.Count; i++)
            {
                var current = arguments[i];
                var expression = Unwrap(current.Expression);

                // A delegate created from a lambda or a method group is not an argument of a dynamic call, which therefore keeps the object result.
                if (current.RefKindKeyword.IsKind(SyntaxKind.None) == false || expression is AnonymousFunctionExpressionSyntax ||
                    expression is SimpleNameSyntax or MemberAccessExpressionSyntax && semanticModel.GetSymbolInfo(expression) is { Symbol: IMethodSymbol } or { CandidateSymbols: [IMethodSymbol, ..] })
                    return null;

                var view = sources.IndexOf(i);
                if (view == -1 && semanticModel.GetConstantValue(expression).HasValue)
                    continue;

                if (view == -1 && semanticModel.GetTypeInfo(expression).Type == null)
                    return null;

                elements.Add((i, view, null));
            }

            var compilation = semanticModel.Compilation;
            var forms = new List<(IMethodSymbol Method, ITypeSymbol[] Types)>();
            foreach (var method in GetCandidates(semanticModel, consumer, semanticModel.GetSymbolInfo(consumer)).Distinct<IMethodSymbol>(SymbolEqualityComparer.Default))
            {
                foreach (var expanded in new[] { false, true })
                {
                    if (GetParameterTypes(method, argumentList, expanded) is { } types &&
                        (method.IsGenericMethod || method.Parameters.Any(x => x.RefKind is RefKind.Ref or RefKind.Out) == false && TakesArguments(types)))
                        forms.Add((method, types));
                }
            }

            // The type arguments of a generic overload are inferred from the results, an extension method is no choice of the dynamic call,
            // and the selected call is an expression, so a method without a result, e.g. in the body of an action, keeps the dynamic call.
            if (forms.Any(x => x.Method.IsGenericMethod || x.Method.ReducedFrom != null || consumer is InvocationExpressionSyntax && x.Method.ReturnsVoid))
                return null;

            var sequences = forms.Where(x => sources.All(i => x.Types[i] is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T }))
                .Select(x => sources.Select(i => x.Types[i]).ToList())
                .ToList();
            var incomparable = Enumerable.Range(0, sources.Count).Any(j => sequences.Select(x => x[j]).Where(IsTypedSequence).ToList() is var views &&
                                                                         views.Any(x => views.Any(y => MayBothMatch(compilation, x, y))));

            var arms = new List<Arm>();
            foreach (var views in sequences)
            {
                if (views.Any(IsTypedSequence) && arms.Any(arm => IsSame(arm.Views, views)) == false)
                    arms.Add(new Arm { Views = views, Excluded = new List<List<(int View, ITypeSymbol Symbol, TypeSyntax Type)>>(), Call = new SyntaxAnnotation(SelectedCall) });
            }

            var typeSyntaxes = new Dictionary<ITypeSymbol, TypeSyntax>(SymbolEqualityComparer.Default);
            foreach (var arm in arms)
            {
                arm.Types = arm.Views.Select(GetTypeSyntax).ToList();
                foreach (var other in sequences)
                {
                    var excluded = Enumerable.Range(0, sources.Count).Where(j => IsView(compilation, arm.Views[j], other[j]) == false).ToList();
                    // An overload the views take binds statically too; one whose views no result can match together with the arm's is no choice.
                    if (excluded.Count == 0 || excluded.Any(j => IsView(compilation, other[j], arm.Views[j]) == false && MayBothMatch(compilation, arm.Views[j], other[j]) == false) ||
                        arm.Excluded.Any(x => x.Select(y => y.View).SequenceEqual(excluded) && IsSame(x.Select(y => y.Symbol).ToList(), excluded.Select(j => other[j]).ToList())))
                        continue;

                    arm.Excluded.Add(excluded.Select(j => (j, other[j], GetTypeSyntax(other[j]))).ToList());
                }

                if (arm.Types.Concat(arm.Excluded.SelectMany(x => x.Select(y => y.Type))).Any(x => x == null))
                    return null;
            }

            // The compiler decides the overload for the views of each arm, alone and together with the views of each other overload the results
            // may match; an arm applies when its overload wins in all of them. Where it cannot decide as the dynamic call, the results must match
            // no other overload.
            var best = arms.Count > 0 && forms.All(x => sources.All(i => IsSequenceOrObject(x.Types[i])))
                ? GetBest(arms.SelectMany(arm => arm.Excluded.Select(excluded => arm.Views.Select((x, j) =>
                        excluded.Where(y => y.View == j).Select(y => (y.Symbol, y.Type)).Prepend((x, arm.Types[j])).ToList()).ToList())
                    .Prepend(arm.Views.Select((x, j) => new List<(ITypeSymbol Symbol, TypeSyntax Type)> { (x, arm.Types[j]) }).ToList())).ToList())
                : null;
            var winners = new Dictionary<Arm, IMethodSymbol>();
            var wholeCall = false;
            if (best != null)
            {
                var probe = 0;
                foreach (var arm in arms)
                {
                    var winner = best[probe++];
                    var spoilers = new List<List<(int View, ITypeSymbol Symbol, TypeSyntax Type)>>();
                    foreach (var excluded in arm.Excluded)
                    {
                        if (SymbolEqualityComparer.Default.Equals(best[probe++], winner) == false)
                            spoilers.Add(excluded);
                    }

                    if (winner != null && forms.Any(x => SymbolEqualityComparer.Default.Equals(x.Method, winner) && IsSame(sources.Select(i => x.Types[i]).ToList(), arm.Views)))
                        winners.Add(arm, winner);

                    wholeCall |= winners.ContainsKey(arm) == false || spoilers.Count > 0;
                    arm.Excluded = spoilers;
                }

                arms.RemoveAll(arm => winners.ContainsKey(arm) == false);
            }

            if (incomparable == false && wholeCall == false)
                return null;

            var calls = new List<(SyntaxAnnotation Call, HashSet<string> Members)>();
            foreach (var arm in arms)
            {
                calls.Add((arm.Call, best != null
                    ? [GetMemberId(winners[arm])]
                    : forms.Where(x => IsSame(sources.Select(i => x.Types[i]).ToList(), arm.Views)).Select(x => GetMemberId(x.Method)).ToHashSet()));
            }

            var facade = new SyntaxAnnotation(SelectedCall);
            calls.Add((facade, semanticModel.GetSymbolInfo(consumer).Symbol is { } written ? [GetMemberId(written)] : null));

            var dynamicType = SyntaxFactory.IdentifierName("dynamic");
            if (consumer is InvocationExpressionSyntax &&
                semanticModel.GetSpeculativeTypeInfo(consumer.SpanStart, dynamicType, SpeculativeBindingOption.BindAsTypeOrNamespace).Type is not { TypeKind: TypeKind.Dynamic })
                return null;

            var captures = 0;
            return new ConsumerPlan
            {
                Consumer = consumer,
                Annotations = results.Select(x => x.Annotation).ToHashSet(),
                Elements = elements.Select(x => (x.Argument, x.View, GetFreshName(names, x.View == -1
                    ? $"__distinctCapture{id}_{captures++}"
                    : sources.Count == 1 ? $"__distinctView{id}" : $"__distinctView{id}_{x.View}"))).ToList(),
                Arms = arms,
                Dynamic = dynamicType,
                Facade = facade,
                Written = sources.Select(GetResultSequences).ToList() is var resultSequences && resultSequences.All(x => x != null) &&
                          arms.All(arm => arm.Excluded.Any(excluded => excluded.All(x => resultSequences[x.View].All(y => IsView(compilation, y, x.Symbol))))),
                Calls = calls
            };

            static ExpressionSyntax Unwrap(ExpressionSyntax expression)
            {
                while (expression is ParenthesizedExpressionSyntax parenthesized)
                    expression = parenthesized.Expression;

                return expression;
            }

            // The sequence types the result of a Distinct call has whatever its source returns, or null when they are known only once the
            // source is evaluated: without a comparer, the dynamic call infers the element type from the runtime type of the source.
            List<ITypeSymbol> GetResultSequences(int i)
            {
                if (Unwrap(arguments[i].Expression) is not InvocationExpressionSyntax distinct || IsAccess(distinct.Expression, DistinctFromEnumerable) == false ||
                    distinct.ArgumentList.Arguments is not [{ Expression: TupleExpressionSyntax { Arguments: [var source, var comparer] } }] ||
                    comparer.Expression.IsKind(SyntaxKind.NullLiteralExpression) == false || Unwrap(source.Expression) is not CastExpressionSyntax cast ||
                    semanticModel.GetTypeInfo(cast).Type is not { TypeKind: TypeKind.Dynamic } ||
                    semanticModel.GetTypeInfo(cast.Expression).Type is not { TypeKind: not (TypeKind.Dynamic or TypeKind.Error) } type)
                    return null;

                var implemented = type.AllInterfaces.Prepend(type)
                    .Where(x => x is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T })
                    .ToList();
                return implemented.Count > 0 && implemented.All(x => IsExact(((INamedTypeSymbol)x).TypeArguments[0])) ? implemented : null;
            }

            // The binder infers from an accessible base type when it cannot access the runtime type, so only a sealed public type keeps the views.
            static bool IsExact(ITypeSymbol type) => type is INamedTypeSymbol named && (named.IsSealed || named.IsValueType) && IsPublic(named);

            static bool IsPublic(INamedTypeSymbol type) =>
                type.DeclaredAccessibility == Accessibility.Public && (type.ContainingType == null || IsPublic(type.ContainingType)) &&
                type.TypeArguments.All(x => x is INamedTypeSymbol argument && IsPublic(argument));

            // As for the dynamic call: a constant converts as written, another argument as a value of its type, and no tuple conversion applies.
            bool TakesArguments(ITypeSymbol[] types)
            {
                for (var i = 0; i < types.Length; i++)
                {
                    if (sources.Contains(i))
                    {
                        // A result is an object of a framework iterator class, which converts to no other class or struct.
                        if (types[i].SpecialType != SpecialType.System_Object && types[i].TypeKind is not (TypeKind.Interface or TypeKind.Dynamic or TypeKind.Error))
                            return false;

                        continue;
                    }

                    var expression = Unwrap(arguments[i].Expression);
                    var type = semanticModel.GetTypeInfo(expression).Type;
                    if (dynamicArguments.Contains(i))
                        continue;

                    // Another dynamic argument is known at runtime only, except an int cast to dynamic, which is an int there.
                    if (type is { TypeKind: TypeKind.Dynamic })
                    {
                        if (GetInt(i) is { } value && TakesValue(semanticModel.GetTypeInfo(value).Type, types[i]) == false)
                            return false;

                        continue;
                    }

                    var conversion = semanticModel.GetConstantValue(expression).HasValue
                        ? semanticModel.ClassifyConversion(expression, types[i]).IsImplicit
                        : TakesValue(type, types[i]);
                    if (conversion == false)
                        return false;
                }

                return true;
            }

            ExpressionSyntax GetInt(int i) =>
                dynamicArguments.Contains(i) == false && Unwrap(arguments[i].Expression) is CastExpressionSyntax cast &&
                semanticModel.GetTypeInfo(cast).Type is { TypeKind: TypeKind.Dynamic } &&
                semanticModel.GetTypeInfo(cast.Expression).Type is { SpecialType: SpecialType.System_Int32 }
                    ? cast.Expression
                    : null;

            // A value converts to a nullable type by the conversion of the underlying types.
            bool TakesValue(ITypeSymbol type, ITypeSymbol target) =>
                compilation.ClassifyConversion(type, target) is { IsImplicit: true, IsTupleConversion: false } conversion &&
                (conversion.IsNullable == false || compilation.ClassifyConversion(GetUnderlyingType(type), GetUnderlyingType(target)).IsTupleConversion == false);

            static ITypeSymbol GetUnderlyingType(ITypeSymbol type) =>
                type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } nullable ? nullable.TypeArguments[0] : type;

            bool IsSame(List<ITypeSymbol> first, List<ITypeSymbol> second) =>
                first.Zip(second, (x, y) => IsView(compilation, x, y) && IsView(compilation, y, x)).All(x => x);

            // The qualified name of a referenced type may also be the name of a type of the index sources, so it must bind to the type.
            TypeSyntax GetTypeSyntax(ITypeSymbol type)
            {
                if (typeSyntaxes.TryGetValue(type, out var syntax))
                    return syntax;

                syntax = SyntaxFactory.ParseTypeName(type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat));
                if (SymbolEqualityComparer.Default.Equals(semanticModel.GetSpeculativeTypeInfo(consumer.SpanStart, syntax, SpeculativeBindingOption.BindAsTypeOrNamespace).Type, type) == false)
                    syntax = null;

                typeSyntaxes[type] = syntax;
                return syntax;
            }

            static bool IsSequenceOrObject(ITypeSymbol type) =>
                type.SpecialType is SpecialType.System_Object or SpecialType.System_Collections_IEnumerable || type.TypeKind == TypeKind.Dynamic ||
                type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T, TypeArguments: [{ TypeKind: not (TypeKind.Dynamic or TypeKind.Error or TypeKind.TypeParameter) }] };

            // The call is bound at its position with each result as a value of a type parameter constrained to the views, which converts to the
            // same parameter types as the result and is exactly none of them.
            List<IMethodSymbol> GetBest(List<List<List<(ITypeSymbol Symbol, TypeSyntax Type)>>> states)
            {
                var function = GetFreshName(names, $"__distinctProbe{id}");
                var typeParameters = sources.Select(i => GetFreshName(names, $"{function}_T{i}")).ToList();
                var parameters = sources.Select(i => GetFreshName(names, $"{function}_{i}")).ToList();
                var probed = sources.ToDictionary(i => (SyntaxNode)arguments[i].Expression, i => parameters[sources.IndexOf(i)]);
                var values = elements.Where(x => x.Argument != -1 && x.View == -1)
                    .Select(x => (arguments[x.Argument].Expression, Value: GetInt(x.Argument) ?? arguments[x.Argument].Expression,
                        Name: GetFreshName(names, $"{function}_{x.Argument}")))
                    .ToList();
                foreach (var (expression, _, name) in values)
                    probed.Add(expression, name);

                var annotations = states.Select(_ => new SyntaxAnnotation()).ToList();
                var probes = SyntaxFactory.Block(values.Select(x => (StatementSyntax)SyntaxFactory.LocalDeclarationStatement(SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("var"), SyntaxFactory.SingletonSeparatedList(SyntaxFactory.VariableDeclarator(x.Name)
                            .WithInitializer(SyntaxFactory.EqualsValueClause(x.Value))))))
                    .Concat(states.Select((state, k) => SyntaxFactory.Block(SyntaxFactory.LocalFunctionStatement(
                            SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.VoidKeyword)), function)
                        .WithTypeParameterList(SyntaxFactory.TypeParameterList(SyntaxFactory.SeparatedList(typeParameters.Select(SyntaxFactory.TypeParameter))))
                        .WithParameterList(SyntaxFactory.ParameterList(SyntaxFactory.SeparatedList(parameters.Select((x, j) =>
                            SyntaxFactory.Parameter(SyntaxFactory.Identifier(x)).WithType(SyntaxFactory.IdentifierName(typeParameters[j]))))))
                        .WithConstraintClauses(SyntaxFactory.List(state.Select((views, j) => SyntaxFactory.TypeParameterConstraintClause(SyntaxFactory.IdentifierName(typeParameters[j]),
                            SyntaxFactory.SeparatedList(views.Select(x => (TypeParameterConstraintSyntax)SyntaxFactory.TypeConstraint(x.Type))
                                .Prepend(SyntaxFactory.ClassOrStructConstraint(SyntaxKind.ClassConstraint)))))))
                        .WithBody(SyntaxFactory.Block(SyntaxFactory.ExpressionStatement(consumer
                            .ReplaceNodes(probed.Keys, (node, _) => SyntaxFactory.IdentifierName(probed[node]).WithTriviaFrom(node))
                            .WithAdditionalAnnotations(annotations[k]))))))))
                    .NormalizeWhitespace();
                if (semanticModel.TryGetSpeculativeSemanticModel(consumer.SpanStart, probes, out var probeModel) == false)
                    return null;

                var overloads = new List<IMethodSymbol>();
                for (var k = 0; k < states.Count; k++)
                {
                    var symbolInfo = probeModel.GetSymbolInfo(probes.GetAnnotatedNodes(annotations[k]).Single());
                    if (symbolInfo.Symbol is IMethodSymbol method && forms.Any(x => SymbolEqualityComparer.Default.Equals(x.Method, method)))
                        overloads.Add(method);
                    // Only an ambiguity among the overloads that take such results leaves no overload better than the others.
                    else if (symbolInfo.Symbol == null && symbolInfo.CandidateReason is CandidateReason.OverloadResolutionFailure or CandidateReason.Ambiguous &&
                             forms.Where(x => sources.Select((i, j) => (Type: x.Types[i], Views: states[k][j]))
                                     .All(y => y.Type is not INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T } ||
                                               y.Views.Any(view => IsView(compilation, view.Symbol, y.Type))))
                                 .Select(x => x.Method).Distinct<IMethodSymbol>(SymbolEqualityComparer.Default).Skip(1).Any())
                        overloads.Add(null);
                    else
                        return null;
                }

                return overloads;
            }
        }

        private const string SelectedCall = nameof(SelectedCall);

        private static bool IsView(Compilation compilation, ITypeSymbol type, ITypeSymbol view)
        {
            var conversion = compilation.ClassifyCommonConversion(type, view);
            return conversion is { IsIdentity: true } or { IsImplicit: true, IsReference: true };
        }

        // A class derives from one class only, so only interface elements may both match; the same holds for a covariant delegate of the
        // result only, e.g. Func<T>, of two interfaces.
        private static bool MayBothMatch(Compilation compilation, ITypeSymbol first, ITypeSymbol second)
        {
            if (IsView(compilation, first, second) || IsView(compilation, second, first))
                return false;

            var firstElement = ((INamedTypeSymbol)first).TypeArguments[0];
            var secondElement = ((INamedTypeSymbol)second).TypeArguments[0];
            return MayBothBe(firstElement, secondElement) ||
                   firstElement is INamedTypeSymbol
                   {
                       ConstructedFrom: { TypeKind: TypeKind.Delegate, TypeParameters: [{ Variance: VarianceKind.Out } result], DelegateInvokeMethod: { Parameters: [] } invoke } definition
                   } firstDelegate &&
                   SymbolEqualityComparer.Default.Equals(invoke.ReturnType, result) &&
                   secondElement is INamedTypeSymbol secondDelegate && SymbolEqualityComparer.Default.Equals(secondDelegate.ConstructedFrom, definition) &&
                   firstDelegate.TypeArguments[0].TypeKind == TypeKind.Interface && secondDelegate.TypeArguments[0].TypeKind == TypeKind.Interface;

            static bool MayBothBe(ITypeSymbol first, ITypeSymbol second) =>
                first.IsReferenceType && second.IsReferenceType && (first.TypeKind == TypeKind.Interface || second.TypeKind == TypeKind.Interface);
        }

        // The switch is built from compiler-owned nodes with placeholders, so no name written in the index is taken for a generated one.
        private static ExpressionSyntax SelectView(ExpressionSyntax consumer, ConsumerPlan plan, string dynamicEnumerable)
        {
            var arguments = (consumer is InvocationExpressionSyntax invocation ? invocation.ArgumentList : ((ObjectCreationExpressionSyntax)consumer).ArgumentList!).Arguments;
            var values = plan.Elements
                .Select(x => x.Argument == -1 ? ((MemberAccessExpressionSyntax)((InvocationExpressionSyntax)consumer).Expression).Expression : arguments[x.Argument].Expression)
                .ToList();
            var results = plan.Elements.Where(x => x.View != -1).OrderBy(x => x.View).Select(x => x.Name).ToList();
            // The view is selected after the receiver and the arguments up to the last result are evaluated, before the arguments after it.
            var head = plan.Elements.FindLastIndex(x => x.View != -1) + 1;

            var arms = plan.Arms
                .Select(arm => SyntaxFactory.SwitchExpressionArm(Pattern(view => SyntaxFactory.DeclarationPattern(arm.Types[view], Variable(results[view]))),
                    arm.Excluded.Count == 0 ? null : SyntaxFactory.WhenClause(arm.Excluded.Select(NotAll).Aggregate(And)), Placeholder()))
                .Append(SyntaxFactory.SwitchExpressionArm(Pattern(view => SyntaxFactory.VarPattern(Variable(results[view]))), Placeholder()));
            var template = SyntaxFactory.SwitchExpression(Placeholder(), SyntaxFactory.SeparatedList(arms)).NormalizeWhitespace();

            var objectType = SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.ObjectKeyword));
            var evaluated = plan.Elements.Take(head)
                .Select((x, i) => x.View == -1 ? values[i] : SyntaxFactory.CastExpression(objectType, SyntaxFactory.ParenthesizedExpression(values[i])))
                .ToList();
            var facade = SyntaxFactory.ParseExpression($"{dynamicEnumerable}.{nameof(DynamicEnumerable.DistinctResultAsObjects)}");
            if (plan.Written)
            {
                var sources = values.Where((_, i) => plan.Elements[i].View != -1).ToHashSet<SyntaxNode>();
                var call = consumer.ReplaceNodes(sources, (_, source) => SyntaxFactory.InvocationExpression(facade, SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.Argument(SyntaxFactory.CastExpression(objectType, SyntaxFactory.ParenthesizedExpression((ExpressionSyntax)source.WithoutTrivia())))))).WithTriviaFrom(source))
                    .WithAdditionalAnnotations(plan.Facade);
                var written = consumer is InvocationExpressionSyntax ? SyntaxFactory.CastExpression(plan.Dynamic, SyntaxFactory.ParenthesizedExpression(call)) : call;
                return SyntaxFactory.ParenthesizedExpression(written).WithTriviaFrom(consumer).WithAdditionalAnnotations(plan.Annotations);
            }

            var replacements = new Dictionary<SyntaxNode, ExpressionSyntax>
            {
                [template.GoverningExpression] = Governing(evaluated)
            };
            for (var i = 0; i < plan.Arms.Count; i++)
                replacements[template.Arms[i].Expression] = WithTail(Call(plan.Arms[i].Call, view => SyntaxFactory.IdentifierName(results[view]), plan.Elements.Count));
            replacements[template.Arms[plan.Arms.Count].Expression] = Call(plan.Facade, view => SyntaxFactory.InvocationExpression(facade,
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(SyntaxFactory.Argument(SyntaxFactory.IdentifierName(results[view]))))), head);

            var selected = template.ReplaceNodes(replacements.Keys, (original, _) => replacements[original]);
            return SyntaxFactory.ParenthesizedExpression(selected).WithTriviaFrom(consumer).WithAdditionalAnnotations(plan.Annotations);

            static ExpressionSyntax Placeholder() => SyntaxFactory.LiteralExpression(SyntaxKind.DefaultLiteralExpression);

            static ExpressionSyntax Governing(List<ExpressionSyntax> evaluated) =>
                evaluated.Count == 1 ? evaluated[0] : SyntaxFactory.TupleExpression(SyntaxFactory.SeparatedList(evaluated.Select(SyntaxFactory.Argument)));

            ExpressionSyntax WithTail(ExpressionSyntax call)
            {
                if (head == plan.Elements.Count)
                    return call;

                var tail = plan.Elements.Skip(head).ToList();
                var pattern = tail.Count == 1
                    ? (PatternSyntax)SyntaxFactory.VarPattern(Variable(tail[0].Name))
                    : SyntaxFactory.RecursivePattern(null, SyntaxFactory.PositionalPatternClause(SyntaxFactory.SeparatedList(tail
                        .Select(x => SyntaxFactory.Subpattern(SyntaxFactory.VarPattern(Variable(x.Name)))))), null, null);
                var nested = SyntaxFactory.SwitchExpression(Placeholder(), SyntaxFactory.SingletonSeparatedList(SyntaxFactory.SwitchExpressionArm(pattern, Placeholder())))
                    .NormalizeWhitespace();
                var nestedReplacements = new Dictionary<SyntaxNode, ExpressionSyntax>
                {
                    [nested.GoverningExpression] = Governing(values.Skip(head).ToList()),
                    [nested.Arms[0].Expression] = call
                };
                return SyntaxFactory.ParenthesizedExpression(nested.ReplaceNodes(nestedReplacements.Keys, (original, _) => nestedReplacements[original]));
            }

            static SingleVariableDesignationSyntax Variable(string name) => SyntaxFactory.SingleVariableDesignation(SyntaxFactory.Identifier(name));

            static ExpressionSyntax And(ExpressionSyntax left, ExpressionSyntax right) => SyntaxFactory.BinaryExpression(SyntaxKind.LogicalAndExpression, left, right);

            ExpressionSyntax NotAll(List<(int View, ITypeSymbol Symbol, TypeSyntax Type)> excluded) =>
                SyntaxFactory.PrefixUnaryExpression(SyntaxKind.LogicalNotExpression, SyntaxFactory.ParenthesizedExpression(excluded
                    .Select(x => (ExpressionSyntax)SyntaxFactory.BinaryExpression(SyntaxKind.IsExpression, SyntaxFactory.IdentifierName(results[x.View]), x.Type))
                    .Aggregate(And)));

            PatternSyntax Pattern(Func<int, PatternSyntax> result)
            {
                if (head == 1)
                    return result(0);

                return SyntaxFactory.RecursivePattern(null, SyntaxFactory.PositionalPatternClause(SyntaxFactory.SeparatedList(plan.Elements.Take(head)
                    .Select(x => SyntaxFactory.Subpattern(x.View == -1 ? SyntaxFactory.VarPattern(Variable(x.Name)) : result(x.View))))), null, null);
            }

            ExpressionSyntax Call(SyntaxAnnotation annotation, Func<int, ExpressionSyntax> result, int count)
            {
                var bound = values.Zip(plan.Elements, (node, x) => (node, x.View == -1 ? SyntaxFactory.IdentifierName(x.Name) : result(x.View)))
                    .Take(count)
                    .ToDictionary(x => (SyntaxNode)x.node, x => x.Item2);
                var call = consumer.ReplaceNodes(bound.Keys, (node, _) => bound[node].WithTriviaFrom(node)).WithAdditionalAnnotations(annotation);
                return consumer is InvocationExpressionSyntax ? SyntaxFactory.CastExpression(plan.Dynamic, SyntaxFactory.ParenthesizedExpression(call)) : call;
            }
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
