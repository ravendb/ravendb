using System.Collections.Generic;

namespace Raven.Database.Commercial
{
	public class WarningMessagesHolder
	{
		public List<string> Messages
		{
			get; set;
		}

		public WarningMessagesHolder()
		{
			Messages = new List<string>();
		}
	}
}
