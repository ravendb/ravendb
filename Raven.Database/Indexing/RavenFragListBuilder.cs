using System;
using System.Collections.Generic;
using Lucene.Net.Search.Vectorhighlight;

namespace Raven.Database.Indexing
{
	public class RavenFragListBuilder : FragListBuilder
	{
		public static int MARGIN = 6;
		public static int MIN_FRAG_CHAR_SIZE = SimpleFragListBuilder.MARGIN*3;

		static RavenFragListBuilder()
		{
		}

		public FieldFragList CreateFieldFragList(FieldPhraseList fieldPhraseList, int fragCharSize)
		{
			if (fragCharSize < SimpleFragListBuilder.MIN_FRAG_CHAR_SIZE)
			{
				throw new ArgumentException("fragCharSize(" + (object) fragCharSize + ") is too small. It must be " +
				                            (string) (object) SimpleFragListBuilder.MIN_FRAG_CHAR_SIZE + " or higher.");
			}
			else
			{
				FieldFragList fieldFragList = new FieldFragList(fragCharSize);
				var phraseInfoList = new List<FieldPhraseList.WeightedPhraseInfo>();
				var enumerator = fieldPhraseList.phraseList.GetEnumerator();
				var weightedPhraseInfo = (FieldPhraseList.WeightedPhraseInfo) null;
				var num = 0;
				var flag = false;
				while (true)
				{
					do
					{
						if (!flag)
						{
							if (enumerator.MoveNext())
								weightedPhraseInfo = enumerator.Current;
							else
								goto label_15;
						}
						flag = false;
						if (weightedPhraseInfo == null)
							goto label_15;
					} while (weightedPhraseInfo.StartOffset < num);
					phraseInfoList.Clear();
					phraseInfoList.Add(weightedPhraseInfo);
					int startOffset = weightedPhraseInfo.StartOffset - SimpleFragListBuilder.MARGIN < num
						                  ? num
						                  : weightedPhraseInfo.StartOffset - SimpleFragListBuilder.MARGIN;
					int endOffset = startOffset + fragCharSize;
					if (weightedPhraseInfo.EndOffset > endOffset)
						endOffset = weightedPhraseInfo.EndOffset;
					num = endOffset;
					while (enumerator.MoveNext())
					{
						weightedPhraseInfo = enumerator.Current;
						flag = true;
						if (weightedPhraseInfo != null && weightedPhraseInfo.EndOffset <= endOffset)
							phraseInfoList.Add(weightedPhraseInfo);
						else
							break;
					}
					fieldFragList.Add(startOffset, endOffset, phraseInfoList);
				}
				label_15:
				return fieldFragList;
			}
		}
	}
}