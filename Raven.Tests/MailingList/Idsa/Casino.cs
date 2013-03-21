using System;
using System.Collections.Generic;

namespace Raven.Tests.MailingList.Idsa
{
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

			Comments = new List<Comment>();
			Suspensions = new List<Suspension>();
		}

		public Casino(string cityId, string address, string name)
			: this()
		{
			AdditionDate = DateTime.UtcNow;
			CityId = cityId;
			Address = address;
			Title = name;
		}
	}

	public enum CasinoStatus
	{
		Opened = 1,
		Closed = 2
	}

	public class Suspension
	{
		public string Id { get; set; }
		public DateTime DateTime { get; set; }
		public IList<Exemption> Exemptions { get; set; }

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

	public class Comment
	{
		public string Id { get; set; }
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
}