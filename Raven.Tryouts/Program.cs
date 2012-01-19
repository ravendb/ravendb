using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;

namespace ConsoleApplication1
{
	internal class Program
	{
		public class Users_AllProperties : AbstractIndexCreationTask<User>
		{
			public Users_AllProperties()
			{
				Map = users =>
				      from user in users
				      select new
				      {
						Query = AsDocument(user).Select(x=>x.Value)
				      };
			}
		}

		private static void Main()
		{
			
		}

	}

	internal class User
	{
		public string Name { get; set; }
		public int Age { get; set; }
		public string PassportNumber { get; set; }
	}
}