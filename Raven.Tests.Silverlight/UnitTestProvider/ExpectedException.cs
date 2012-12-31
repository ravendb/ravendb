// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.
namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using System.Diagnostics.CodeAnalysis;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	[SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix",
		Justification = "Standard unit test framework naming")]
	public class ExpectedException : IExpectedException
	{
		readonly ExpectedExceptionAttribute exp;

		ExpectedException()
		{
		}

		public ExpectedException(ExpectedExceptionAttribute expectedExceptionAttribute)
		{
			exp = expectedExceptionAttribute;
			if (exp == null)
			{
				throw new ArgumentNullException("expectedExceptionAttribute");
			}
		}

		public Type ExceptionType
		{
			get { return exp.ExceptionType; }
		}

		public string Message
		{
			get { return exp.Message; }
		}
	}
}