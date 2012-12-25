using System.Text.RegularExpressions;
using System.Collections.Generic;
using StyleCop;
using StyleCop.CSharp;

namespace Raven.CustomStyleCopRules
{
	[SourceAnalyzer(typeof(CsParser))]
	public class IndentationRules : SourceAnalyzer
	{
		public override void AnalyzeDocument(CodeDocument document)
		{
			Param.RequireNotNull(document, "document");

			var csdocument = (CsDocument)document;
			if (csdocument.RootElement != null && !csdocument.RootElement.Generated)
			{
				CheckSpacing(csdocument.Tokens);
			}
		}

		private void CheckSpacing(IEnumerable<CsToken> tokens)
		{
			Param.AssertNotNull(tokens, "tokens");

			foreach (var token in tokens)
			{
				if (Cancel) break;
				if (token.Generated) continue;
				
				switch (token.CsTokenType)
				{
					case CsTokenType.WhiteSpace:
						CheckWhitespace(token as Whitespace);
						break;
				}
			}
		}

		private void CheckWhitespace(Whitespace whitespace)
		{
			Param.AssertNotNull(whitespace, "whitespace");

			// Match any leading spaces, but not "  *", which might be used in block comments that are not indented
			if (whitespace.Location.StartPoint.IndexOnLine == 0 && Regex.IsMatch(whitespace.Text, @"^(?!  \*) +"))
			{
				AddViolation(whitespace.FindParentElement(), whitespace.LineNumber, "TabsMustBeUsed");
			}
		}
	}
}
