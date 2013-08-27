// -----------------------------------------------------------------------
//  <copyright file="DebugIndexFields.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using ICSharpCode.NRefactory.CSharp;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Debugging
{
    public class DebugIndexFields : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get { return @"^/debug/index-fields"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

        public override void Respond(IHttpContext context)
        {
            var indexStr = context.ReadString();
            VariableInitializer mapDefinition = indexStr.Trim().StartsWith("from")
                ? QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexStr, true)
                : QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexStr, true);

            var captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor();
            mapDefinition.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);

            context.WriteJson(new { captureSelectNewFieldNamesVisitor.FieldNames });
        }
    }
}