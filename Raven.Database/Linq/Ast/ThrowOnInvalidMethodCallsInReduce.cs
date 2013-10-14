// -----------------------------------------------------------------------
//  <copyright file="ThrowOnInvalidMethodCallsInReduce.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Linq.Ast
{
	using System;
	using System.Linq;

	using ICSharpCode.NRefactory.CSharp;

	public class ThrowOnInvalidMethodCallsInReduce : ThrowOnInvalidMethodCalls
	{
		public ThrowOnInvalidMethodCallsInReduce(string groupByIdentifier)
			: base(groupByIdentifier)
		{
		}

		protected override void AssertInvocationExpression(InvocationExpression invocation)
		{
			var memberReferenceExpression = invocation.Target as MemberReferenceExpression;
			if (memberReferenceExpression != null)
			{
				if (memberReferenceExpression.MemberName == "LoadDocument")
					throw new InvalidOperationException("Reduce cannot contain LoadDocument() methods.");
			}

			base.AssertInvocationExpression(invocation);
		}

		protected override void AssertInvocationExpression(InvocationExpression invocation, ParameterDeclaration parameter)
		{
			foreach (var member in invocation.Descendants.OfType<MemberReferenceExpression>())
			{
				if (member.MemberName == "LoadDocument")
					throw new InvalidOperationException("Reduce cannot contain LoadDocument() methods.");
			}

			base.AssertInvocationExpression(invocation, parameter);
		}
	}
}