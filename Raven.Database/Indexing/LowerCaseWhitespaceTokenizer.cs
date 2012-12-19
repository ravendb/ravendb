using System.IO;

namespace Raven.Database.Indexing
{
	public class LowerCaseWhitespaceTokenizer : LowerCaseKeywordTokenizer
	{
		public LowerCaseWhitespaceTokenizer(TextReader input) : base(input)
		{
		}

		protected internal override bool IsTokenChar(char c)
		{
			return char.IsWhiteSpace(c) == false;
		}
	}
}