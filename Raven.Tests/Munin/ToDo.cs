using System;

namespace Raven.Munin.Tests
{
	public class ToDo
	{
		public string Action { get; set; }
		public DateTime Date { get; set; }

		public override string ToString()
		{
			return Action;
		}
	}
}