//-----------------------------------------------------------------------
// <copyright file="ExpressionOperatorPrecedenceTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes
{
	public class ExpressionOperatorPrecedenceTest : NoDisposalNeeded
	{
		[Fact]
		public void operators_of_same_precedence_do_need_parenthesis()
		{
			Assert.True(ExpressionOperatorPrecedence.Multiplicative.NeedsParenthesisFor(ExpressionOperatorPrecedence.Multiplicative));
		}

		[Fact]
		public void detects_when_parenthesis_is_needed()
		{
			Assert.True(ExpressionOperatorPrecedence.Multiplicative.NeedsParenthesisFor(ExpressionOperatorPrecedence.Additive));
			Assert.True(ExpressionOperatorPrecedence.Unary.NeedsParenthesisFor(ExpressionOperatorPrecedence.Multiplicative));
			Assert.True(ExpressionOperatorPrecedence.Unary.NeedsParenthesisFor(ExpressionOperatorPrecedence.Equality));
		}

		[Fact]
		public void detects_when_parenthesis_not_needed()
		{
			Assert.False(ExpressionOperatorPrecedence.Additive.NeedsParenthesisFor(ExpressionOperatorPrecedence.Multiplicative));
			Assert.False(ExpressionOperatorPrecedence.Additive.NeedsParenthesisFor(ExpressionOperatorPrecedence.Unary));
			Assert.False(ExpressionOperatorPrecedence.Multiplicative.NeedsParenthesisFor(ExpressionOperatorPrecedence.Unary));
			Assert.False(ExpressionOperatorPrecedence.Conditional.NeedsParenthesisFor(ExpressionOperatorPrecedence.RelationalAndTypeTesting)); // learn something new every day
		}

		[Fact]
		public void have_pseudo_operator()
		{
			Assert.False(ExpressionOperatorPrecedence.ParenthesisNotNeeded.NeedsParenthesisFor(ExpressionOperatorPrecedence.Additive));
			Assert.False(ExpressionOperatorPrecedence.ParenthesisNotNeeded.NeedsParenthesisFor(ExpressionOperatorPrecedence.Multiplicative));
			Assert.False(ExpressionOperatorPrecedence.Multiplicative.NeedsParenthesisFor(ExpressionOperatorPrecedence.ParenthesisNotNeeded));
			Assert.False(ExpressionOperatorPrecedence.Multiplicative.NeedsParenthesisFor(ExpressionOperatorPrecedence.ParenthesisNotNeeded));
		}

	}
}
