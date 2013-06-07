// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.
namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System.Globalization;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;

	public class Priority : IPriority
	{
		public Priority(int priority)
		{
			Value = priority;
		}

		public int Value { get; private set; }


		public override string ToString()
		{
			return Value.ToString(CultureInfo.InvariantCulture);
		}
	}
}