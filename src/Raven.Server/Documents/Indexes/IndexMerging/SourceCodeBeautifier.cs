using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.IndexMerging;

internal static class SourceCodeBeautifier
{
    public static FormattedExpression FormatIndex(string sourceCode)
    {
        var type = IndexDefinitionHelper.DetectStaticIndexType(sourceCode, reduce: null);

        FormattedExpression formattedExpression;
        switch (type)
        {
            case IndexType.Map:
            case IndexType.MapReduce:
                using (var workspace = new AdhocWorkspace())
                {
                    var expression = SyntaxFactory
                        .ParseExpression(sourceCode)
                        .NormalizeWhitespace();

                    var result = Formatter.Format(expression, workspace);

                    if (result.ToString().IndexOf("Could not format:", StringComparison.Ordinal) > -1)
                        throw new BadRequestException();

                    formattedExpression = new FormattedExpression
                    {
                        Expression = result.ToString()
                    };
                }
                break;

            case IndexType.JavaScriptMap:
            case IndexType.JavaScriptMapReduce:
                formattedExpression = new FormattedExpression
                {
                    Expression = JSBeautify.Apply(sourceCode)
                };
                break;

            default:
                throw new NotSupportedException($"Unknown index type '{type}'.");
        }

        return formattedExpression;
    }
    
    public sealed class FormattedExpression : IDynamicJson
    {
        public string Expression { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Expression)] = Expression
            };
        }
    }
}
