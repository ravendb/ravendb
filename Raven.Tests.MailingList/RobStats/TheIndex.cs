using System;
using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.MailingList.RobStats
{
	class TheIndex : AbstractMultiMapIndexCreationTask<Summary>
	{
		public TheIndex()
		{
			AddMap<Opinion>(
				opinions => from opinion in opinions
							select new
							{
								opinion.EntityId,
								DisplayName = (string)null,
								Visibility = (string)null,
								UpdatedAt = DateTimeOffset.MinValue,
								NumberOfFavorites = opinion.IsFavorite ? 1 : 0,
							});

			AddMap<Entity>(
				entities => from entity in entities
							select new
							{
								EntityId = entity.Id,
								entity.DisplayName,
								entity.Visibility,
								entity.UpdatedAt,
								NumberOfFavorites = 0,
							});

			Reduce = results => from result in results
								group result by result.EntityId
									into g
									select new
									{
										EntityId = g.Key,
										DisplayName = g.Select(x => x.DisplayName).Where(x => x != null).FirstOrDefault(),
										Visibility = g.Select(x => x.Visibility).Where(x => x != null).FirstOrDefault(),
										UpdatedAt = g.Max(x => (DateTimeOffset)x.UpdatedAt),
										NumberOfFavorites = g.Sum(x => x.NumberOfFavorites),
									};
		}
	}
}