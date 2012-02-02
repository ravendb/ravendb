using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Alexander : RavenTest
	{
		[Fact]
		public void QueryById()
		{
			using (GetNewServer())
			using (
				var documentStore = new DocumentStore()
				{Url = "http://localhost:8079", Conventions = {DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites}})
			{
				documentStore.Initialize();

				var documentSession = documentStore.OpenSession();

				var casino = new Casino("Cities/123456", "address", "name");
				documentSession.Store(casino);
				documentSession.SaveChanges();

				var casinoFromDb = documentSession.Query<Casino>().Where(x => x.Id == casino.Id).Single();
				Assert.NotNull(casinoFromDb);
			}
		}
	}

	public class Casino
	{
		public string Id { get; set; }
		public DateTime AdditionDate { get; set; }
		public string CityId { get; set; }
		public string Address { get; set; }
		public string Title { get; set; }
		public CasinoStatus Status { get; set; }
		public IList<Comment> Comments { get; set; }
		public IList<Suspension> Suspensions { get; set; }

		private Casino()
		{
			Status = CasinoStatus.Opened;
		}

		public Casino(string cityId, string address, string name)
			: this()
		{
			AdditionDate = DateTime.UtcNow;
			CityId = cityId;
			Address = address;
			Title = name;

			Comments = new List<Comment>();
			Suspensions = new List<Suspension>();
		}
	}

	public enum CasinoStatus
	{
		Opened = 1,
		Closed = 2
	}

	public class Comment
	{
		public DateTime DateTime { get; set; }
		public string Author { get; set; }
		public string Text { get; set; }

		public Comment(string author, string text)
		{
			DateTime = DateTime.UtcNow;
			Author = author;
			Text = text;
		}
	}

	public class Suspension
	{
		public DateTime DateTime { get; set; }
		public IList<Exemption> Exemptions { get; set; }

		public Suspension()
		{
			Exemptions = new List<Exemption>();
		}

		public Suspension(DateTime dateTime, IList<Exemption> exemptions)
		{
			DateTime = dateTime;
			Exemptions = exemptions;
		}
	}

	public class Exemption
	{
		public ExemptionItemType ItemType { get; set; }
		public long Quantity { get; set; }

		public Exemption(ExemptionItemType itemType, long quantity)
		{
			ItemType = itemType;
			Quantity = quantity;
		}
	}

	public enum ExemptionItemType
	{
		Unknown = 1,
		Pc = 2,
		SlotMachine = 3,
		Table = 4,
		Terminal = 5

	}

}