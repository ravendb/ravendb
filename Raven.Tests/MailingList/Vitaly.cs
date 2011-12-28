using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Vitaly
	{
		public class ActivityShot
		{
			public byte[] Thumbnail { get; set; }

			public DateTimeOffset Edited { get; set; }
		}

		public class DailyActivity
		{
			public DateTime Date { get; set; }

			public byte[][] Thumbnails
			{
				get;
				set;
			}
		}


		public class DailyActivityIndex : AbstractIndexCreationTask<ActivityShot, DailyActivity>
		{
			public DailyActivityIndex()
			{
				Map = activityShots => from shot in activityShots
									   select new
									   {
										   Date = shot.Edited.Date,
										   Thumbnails = new byte[][] { shot.Thumbnail }
									   };

				Reduce = results => from result in results
									group result by result.Date into g
									select new
									{
										Date = g.Key,
										Thumbnails = from dailyActivity in g
													 from thumbnail in dailyActivity.Thumbnails
													 select thumbnail
									};
			}

		}

		[Fact]
		public void Test()
		{
			var activityShot1 = new ActivityShot
									{
										Edited = new DateTime(2011, 1, 1),
										Thumbnail = new byte[] { 1 }
									};

			var activityShot2 = new ActivityShot
									{
										Edited = new DateTime(2011, 10, 10),
										Thumbnail = new byte[] { 2 }
									};

			using (var documentStore = new EmbeddableDocumentStore
										   {
											   RunInMemory = true
										   }.Initialize())
			{
				new DailyActivityIndex().Execute(documentStore);	

				using (var session = documentStore.OpenSession())
				{
					session.Store(activityShot1);
					session.Store(activityShot2);

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					session.Query<DailyActivity, DailyActivityIndex>().Customize(x => x.WaitForNonStaleResults()).ToArray();
				}
			}

		}
	}
}