//-----------------------------------------------------------------------
// <copyright file="ExpressionOperatorPrecedenceExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Client.Indexes
{
	/// <summary>
	/// Methods on the enum
	/// </summary>
	public static class ExpressionOperatorPrecedenceExtension
	{
		/// <summary>
		/// Needs parenthesis for the expression
		/// </summary>
		public static bool NeedsParenthesisFor(this ExpressionOperatorPrecedence outer, ExpressionOperatorPrecedence inner)
		{
			if (outer == ExpressionOperatorPrecedence.ParenthesisNotNeeded || inner == ExpressionOperatorPrecedence.ParenthesisNotNeeded)
				return false;

			return outer > inner;
		}
	}
}