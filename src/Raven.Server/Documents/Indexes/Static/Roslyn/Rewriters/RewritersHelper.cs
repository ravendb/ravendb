using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public static class RewritersHelper
    {
        public static HashSet<string> ExtractFields(AnonymousObjectCreationExpressionSyntax anonymousObjectCreationExpressionSyntax, bool retrieveOriginal = false)
        {
            var fields = new HashSet<string>();
            for (var i = 0; i < anonymousObjectCreationExpressionSyntax.Initializers.Count; i++)
            {
                var initializer = anonymousObjectCreationExpressionSyntax.Initializers[i];
                string name;
                if (initializer.NameEquals != null && retrieveOriginal == false)
                {
                    name = initializer.NameEquals.Name.Identifier.Text;
                }
                else
                {
                    var expressionSysntaxMemberAccess = initializer.Expression as MemberAccessExpressionSyntax;
                    if (expressionSysntaxMemberAccess != null)
                    {
                        name = expressionSysntaxMemberAccess.Name.Identifier.Text;
                    }
                    else
                    {
                        var identifierNameSyntax = initializer.Expression as IdentifierNameSyntax;

                        if (identifierNameSyntax == null)
                            throw new NotSupportedException($"Cannot extract field name from: {initializer}");

                        name = identifierNameSyntax.Identifier.Text;
                    }
                }

                fields.Add(name.TrimStart('@'));
            }

            return fields;
        }
    }
}
