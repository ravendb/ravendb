// -----------------------------------------------------------------------
//  <copyright file="Jonas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Jonas : RavenTest
	{
		[Fact]
		public void CanCreateIndexWithGenerics()
		{
			using(var store = NewDocumentStore())
			{
				new PlayerScore_Distribution().Execute(store);
			}
		}

		public class PlayerScore_Distribution : AbstractIndexCreationTask<Round, PlayerScore_Distribution.ScoreDistribution>
		{
			public class ScoreDistribution
			{
				public string PlayerName { get; set; }

				public string CourseId { get; set; }

				public int HoleNumber { get; set; }

				public Dictionary<int, int> Distribution { get; set; }
			}

			public PlayerScore_Distribution()
			{
				this.Map = rounds =>
					from round in rounds
					from playerRound in round.PlayerRounds
					from score in playerRound.Scores
					select new
					{
						round.CourseId,
						playerRound.PlayerName,
						score.HoleNumber,
						Distribution = new Dictionary<int, int> { { score.Strokes, 1 } },
					};

				this.Reduce = results =>
					from result in results
					group result by new { result.CourseId, result.HoleNumber, result.PlayerName } into g
					select new
					{
						g.Key.CourseId,
						g.Key.HoleNumber,
						g.Key.PlayerName,
						Distribution = g
							.SelectMany(x => x.Distribution)
							.GroupBy(x => x.Key)
							.ToDictionary(x => x.Key, x => x.Sum(y => y.Value)),
					};
			}
		}

		public class Round
		{
			public PlayerRound[] PlayerRounds { get; set; }
			public object CourseId { get; set; }

			public class PlayerRound
			{
				public IEnumerable<Score> Scores { get; set; }
				public string PlayerName { get; set; }

				public class Score
				{
					public int Strokes { get; set; }
					public string HoleNumber { get; set; }
				}
			}
		}
	}


}