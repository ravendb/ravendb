using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Samples.IndexReplicationToRedis.PocoTypes
{
	public class QuestionSummary
	{
		public string Id { get; set; }
		public string Title { get; set; }

		public DateTime QuestionDate { get; set; }

		public int UpVotes { get; set; }
		public int DownVotes { get; set; }

		public double SumPoints { get; set; }
	}

}
