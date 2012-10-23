using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq
{
	public class StringLiteralExpression : PrimitiveExpression
	{
		public StringLiteralExpression(string value)
			: base(value, '"' + value + '"')
		{
		}
	}
}