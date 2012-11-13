using System;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq
{
	[CLSCompliant(false)]
	public class StringLiteralExpression : PrimitiveExpression
	{
		public StringLiteralExpression(string value)
			: base(value, '"' + value + '"')
		{
		}
	}
}