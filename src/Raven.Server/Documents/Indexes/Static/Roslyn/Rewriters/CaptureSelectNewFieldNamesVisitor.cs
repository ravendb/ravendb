using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class CaptureSelectNewFieldNamesVisitor : CSharpSyntaxRewriter
    {
        public HashSet<CompiledIndexField> Fields;

        public static HashSet<string> KnownMethodsToInspect = new HashSet<string>
        {
            "Select",
            "SelectMany",
            "Boost",
            "GroupBy",
            "OrderBy",
            "Distinct",
            "Where"
        };
        
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            var mae = node.Expression as MemberAccessExpressionSyntax;
            if (mae == null)
                return Visit(node.Expression);

            if (KnownMethodsToInspect.Contains(mae.Name.Identifier.Text) == false)
                return Visit(node.Expression);

            CaptureFieldNames(node, x => x.VisitInvocationExpression(node));

            return node;
        }

        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            if (Fields != null)
                return node;

            CaptureFieldNames(node, x => x.VisitQueryBody(node));

            return node;
        }

        private void CaptureFieldNames(SyntaxNode node, Action<CaptureDictionaryFieldsNamesVisitor> visitDictionaryNodeExpression)
        {
            var nodes = node.DescendantNodes(descendIntoChildren: syntaxNode =>
            {
                if (syntaxNode is AnonymousObjectCreationExpressionSyntax ||
                    syntaxNode is ObjectCreationExpressionSyntax oce && CaptureDictionaryFieldsNamesVisitor.IsDictionaryObjectCreationExpression(oce))
                {
                    return false;
                }
                return true;
            }).ToList();

            var lastObjectCreation = nodes.LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression) ||
                                                              x.IsKind(SyntaxKind.ObjectCreationExpression) &&
                                                              x is ObjectCreationExpressionSyntax oce &&
                                                              CaptureDictionaryFieldsNamesVisitor.IsDictionaryObjectCreationExpression(oce));

            if (lastObjectCreation is AnonymousObjectCreationExpressionSyntax lastAnonymousObjectCreation)
            {
                VisitAnonymousObjectCreationExpression(lastAnonymousObjectCreation);
            }
            else if (lastObjectCreation is ObjectCreationExpressionSyntax oce && CaptureDictionaryFieldsNamesVisitor.IsDictionaryObjectCreationExpression(oce))
            {
                var dictVisitor = new CaptureDictionaryFieldsNamesVisitor();

                visitDictionaryNodeExpression(dictVisitor);

                Fields = dictVisitor.Fields;
            }
            else
            {
                ThrowIndexingFunctionMustReturnAnonymousObjectOrDictionary();
            }
        }

        public override SyntaxNode VisitAnonymousObjectCreationExpression(AnonymousObjectCreationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            Fields = RewritersHelper.ExtractFields(node);

            return node;
        }

        public override SyntaxNode VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            if (CaptureDictionaryFieldsNamesVisitor.IsDictionaryObjectCreationExpression(node))
            {
                var dictVisitor = new CaptureDictionaryFieldsNamesVisitor();
                dictVisitor.VisitObjectCreationExpression(node);
                Fields = dictVisitor.Fields;
            }

            return node;
        }

        private static void ThrowIndexingFunctionMustReturnAnonymousObjectOrDictionary()
        {
            throw new InvalidOperationException($"Indexing function must return an anonymous object or {CaptureDictionaryFieldsNamesVisitor.SupportedGenericDictionaryType}");
        }
    }
}
