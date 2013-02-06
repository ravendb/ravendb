using System.Collections.Generic;
using Lucene.Net.Search.Vectorhighlight;

namespace Raven.Database.Indexing
{
	public class RavenFragmentsBuilder : BaseFragmentsBuilder
	{
		/// <summary>
		/// a constructor.
		/// 
		/// </summary>
		public RavenFragmentsBuilder()
		{
		}

		/// <summary>
		/// a constructor.
		/// 
		/// </summary>
		/// <param name="preTags">array of pre-tags for markup terms</param><param name="postTags">array of post-tags for markup terms</param>
		public RavenFragmentsBuilder(string[] preTags, string[] postTags)
			: base(preTags, postTags)
		{
		}

		/// <summary>
		/// do nothing. return the source list.
		/// 
		/// </summary>
		public override List<FieldFragList.WeightedFragInfo> GetWeightedFragInfoList(List<FieldFragList.WeightedFragInfo> src)
		{
			return src;
		}
	}
}