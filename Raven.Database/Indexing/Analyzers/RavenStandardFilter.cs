using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;

namespace Raven.Database.Indexing.Analyzers
{
	public class RavenStandardFilter : TokenFilter
	{
		public RavenStandardFilter(TokenStream input) : base(input)
		{
			termAtt = AddAttribute<ITermAttribute>();
			typeAtt = AddAttribute<ITypeAttribute>();
			innerInputStream = input;
		}

		private TokenStream innerInputStream;
		private const String APOSTROPHE_TYPE = "<APOSTROPHE>";
		private const String ACRONYM_TYPE = "<ACRONYM>";

		private ITypeAttribute typeAtt;
		private ITermAttribute termAtt;
		public override bool IncrementToken()
		{			
			if (!input.IncrementToken())
			{
				return false;
			}
			bool bufferUpdated = true;
			char[] buffer = termAtt.TermBuffer();
			int bufferLength = termAtt.TermLength();
			String type = typeAtt.Type;

			if (type == APOSTROPHE_TYPE && bufferLength >= 2 && buffer[bufferLength - 2] == '\'' && (buffer[bufferLength - 1] == 's' || buffer[bufferLength - 1] == 'S'))
			{
				for (int i = 0; i < bufferLength - 2; i++)
				{
					buffer[i] = ToLower(buffer[i]);
				}
				// Strip last 2 characters off
				termAtt.SetTermLength(bufferLength - 2);				
			}
			else if (type == ACRONYM_TYPE)
			{
				// remove dots
				int upto = 0;
				for (int i = 0; i < bufferLength; i++)
				{
					char c = buffer[i];
					if (c != '.')
						buffer[upto++] = ToLower(c);
				}
				termAtt.SetTermLength(upto);
			}
			else
			{
				do
				{
					//If we consumed a stop word we need to update the buffer and its length.
					if (!bufferUpdated)
					{
						bufferLength = termAtt.TermLength();
						buffer = termAtt.TermBuffer();
					}
					
					for (int i = 0; i < bufferLength; i++)
					{
						buffer[i] = ToLower(buffer[i]);
					}
					if (!stopWords.Contains(buffer, 0, bufferLength))
					{						
						return true;
					}
					bufferUpdated = false;
				} while (input.IncrementToken());
				return false;
			}
			return true;
		}

		protected char ToLower(char c)
		{
			int cInt = c;

			if (c < 128 && isAsciiCasingSameAsInvariant)
			{
				if (65 <= cInt && cInt <= 90)
					c |= ' ';

				return c;
			}
			return invariantTextInfo.ToLower(c);

		}

		public bool Reset(System.IO.TextReader reader)
		{
			var input = (StandardTokenizer) innerInputStream;
			if (input != null)
			{
				input.Reset(reader);
				return true;
			}
			return false;		
		}

		private static readonly bool isAsciiCasingSameAsInvariant = CultureInfo.InvariantCulture.CompareInfo.Compare("abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", CompareOptions.IgnoreCase) == 0;
		private static readonly TextInfo invariantTextInfo = CultureInfo.InvariantCulture.TextInfo;
		private readonly CharArraySet stopWords = new CharArraySet(StopAnalyzer.ENGLISH_STOP_WORDS_SET, false);
	}
}
