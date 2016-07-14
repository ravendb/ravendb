using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public static class RewritersHelper
    {
        public static HashSet<string> ExtractFields(AnonymousObjectCreationExpressionSyntax anonymousObjectCreationExpressionSyntax)
        {
            var fields = new HashSet<string>();
            for (var i = 0; i < anonymousObjectCreationExpressionSyntax.Initializers.Count; i++)
            {
                var initializer = anonymousObjectCreationExpressionSyntax.Initializers[i];
                string name;
                if (initializer.NameEquals != null)
                {
                    name = initializer.NameEquals.Name.Identifier.Text;
                }
                else
                {
                    var memberAccess = initializer.Expression as MemberAccessExpressionSyntax;
                    if (memberAccess == null)
                        throw new NotSupportedException($"Cannot extract field name from: {initializer}");

                    name = memberAccess.Name.Identifier.Text;
                }

                fields.Add(name);
            }

            return fields;
        }
    }
}