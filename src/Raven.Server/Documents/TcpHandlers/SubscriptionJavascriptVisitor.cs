using System;
using System.Text;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.TcpHandlers;

public sealed class SubscriptionJavascriptVisitor : JavascriptCodeQueryVisitor
{
    public SubscriptionJavascriptVisitor(StringBuilder sb, Query q) : base(sb, q)
    {
    }

    protected override void OnBeforeVisitMethod(MethodExpression expr)
    {
        if (expr.Name.Value.Equals("now", StringComparison.OrdinalIgnoreCase) ||
            expr.Name.Value.Equals("today", StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException($"'{expr.Name.Value}()' function is not supported in subscriptions");
        }
    }
}
