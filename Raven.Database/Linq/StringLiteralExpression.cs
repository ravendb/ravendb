using System;
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

	[CLSCompliant(false)]
	public class VerbatimStringLiteralExpression : PrimitiveExpression
	{
		public VerbatimStringLiteralExpression(string value)
			: base(value, "@" + '"' + value.Replace("\"", "\"\"") + '"')
		{
		}
	}
}