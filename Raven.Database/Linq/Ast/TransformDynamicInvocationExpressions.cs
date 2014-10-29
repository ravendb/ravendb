// -----------------------------------------------------------------------
//  <copyright file="TransformDynamicInvocationExpressions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq.Ast
{
    [CLSCompliant(false)]
    public class TransformDynamicInvocationExpressions : DepthFirstAstVisitor<object, object>
    {
        public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
        {
            var parentInvocationExpression = invocationExpression.Parent as InvocationExpression;

            if (parentInvocationExpression == null)
                return base.VisitInvocationExpression(invocationExpression, data);

            var target = invocationExpression.Target as MemberReferenceExpression;
            if (target == null)
                return base.VisitInvocationExpression(invocationExpression, data);

            switch (target.MemberName)
            {
                case "Range":
                    var identifierExpression = target.Target as IdentifierExpression;
                    if (identifierExpression != null && identifierExpression.Identifier == "Enumerable")
                    {
                        var parentTarget = parentInvocationExpression.Target as MemberReferenceExpression;

                        if (parentTarget == null)
                            break;

                        if (parentTarget.MemberName == "ToDictionary" &&
                            parentInvocationExpression.Arguments.Count == 3 &&
                            parentInvocationExpression.Arguments.First() == invocationExpression)
                        {
                            // support for Enumerable.Range(x, y).ToDictionary(k => k, k => k) which is  
                            // Enumerable.ToDictionary(Enumerable.Range(x, y), k => k, k => k) in fact      

                            var toDictArguments = parentInvocationExpression.Arguments.Skip(1); // skip first arg which is current invocationExpression

                            var containsDynamics = toDictArguments.All(x =>
                            {
                                var castExpression = x as CastExpression;
                                if (castExpression == null)
                                    return false;
                                var type = castExpression.Type as SimpleType;
                                if (type == null)
                                    return false;
                                return type.Identifier.Contains("dynamic");
                            });

                            if (containsDynamics == false)
                                break;

                            // convert Enumerable.Range(x, y) to Enumerable.Range(x, y).Cast<dynamic>()
                            var enumerableRange = invocationExpression.Clone();

                            var castToDynamic = new MemberReferenceExpression(enumerableRange, "Cast", new AstType[] { new SimpleType("dynamic") });

                            var dynamicEnumerable = new InvocationExpression(castToDynamic);

                            invocationExpression.ReplaceWith(dynamicEnumerable);
                        }
                    }
                    break;
            }

            return base.VisitInvocationExpression(invocationExpression, data);
        }
    }
}