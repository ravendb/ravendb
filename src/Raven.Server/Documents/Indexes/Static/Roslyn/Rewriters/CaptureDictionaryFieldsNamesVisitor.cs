using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class CaptureDictionaryFieldsNamesVisitor : CSharpSyntaxRewriter
    {
        public const string SupportedGenericDictionaryType = "Dictionary<string, object>";

        public HashSet<Field> Fields;

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            CaptureFieldNames(node);

            return node;
        }

        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            CaptureFieldNames(node);

            return node;
        }

        private void CaptureFieldNames(SyntaxNode node)
        {
            if (Fields != null)
                return;

            var lastObjectCreation = node.DescendantNodes(descendIntoChildren: syntaxNode =>
                {
                    if (syntaxNode is ObjectCreationExpressionSyntax)
                    {
                        return false;
                    }
                    return true;
                })
                .LastOrDefault(x => x.IsKind(SyntaxKind.ObjectCreationExpression)) as ObjectCreationExpressionSyntax;

            if (lastObjectCreation != null && IsDictionaryObjectCreationExpression(lastObjectCreation))
            {
                VisitObjectCreationExpression(lastObjectCreation);
            }
        }

        public override SyntaxNode VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            Fields = new HashSet<Field>();

            if (IsDictionaryObjectCreationExpression(node) == false)
            {
                return node;
            }

            var initializerExpressionSyntax = node.Initializer;

            foreach (var keyValuePairExpressions in initializerExpressionSyntax.Expressions)
            {
                var initializerExpression = keyValuePairExpressions as InitializerExpressionSyntax;

                if (initializerExpression == null)
                    throw new InvalidOperationException("Dictionary has to define indexed properties using a collection initializer");

                if (initializerExpression.Expressions.Count != 2)
                    throw new InvalidOperationException($"Expected to get 2 expression but got {initializerExpression.Expressions.Count}: {initializerExpression}");

                var dictionaryKey = initializerExpression.Expressions[0] as LiteralExpressionSyntax;

                if (dictionaryKey == null)
                    throw new InvalidOperationException($"Dictionary returned in an index definition has to be '{SupportedGenericDictionaryType}'");

                if (Fields == null)
                    Fields = new HashSet<Field>();

                if (Fields.Add(new SimpleField(dictionaryKey.Token.Value.ToString())) == false)
                    throw new InvalidOperationException("Duplicated field name in indexed dictionary");
            }

            return node;
        }

        public static bool IsDictionaryObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            GenericNameSyntax genericObjectCreation = null;

            if (node.Type is QualifiedNameSyntax qns)
            {
                genericObjectCreation = qns.Right as GenericNameSyntax;
            }
            else
            {
                genericObjectCreation = node.Type as GenericNameSyntax;
            }

            if (genericObjectCreation == null)
                return false;

            return genericObjectCreation.ToString() == SupportedGenericDictionaryType;
        }
    }
}
