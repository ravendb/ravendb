// -----------------------------------------------------------------------
//  <copyright file="Vicente.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client;
using Xunit;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class Vicente : RavenTest
	{
		public const string query =
			@"CTOTurning a dream into a web :)
https://www.talentous.comSenior consultantWorking as software architect and senior developer in Raona key accounts.Software Design Engineer•	In this company, I worked as software design engineer developing software projects with Microsoft Visual Studio 2008/2010, SQL Server 2008, Visual C# & LINQ and Entity Framework. I participated in the whole software project lifecycle, performing design, implementation and modification tasks. Also, I did load testing and software debug and optimization. 

•	Full customer satisfaction through technical and personal skills demonstrated.

•	Attendance at several internal training workshops on advanced use of Microsoft Entity Framework and N-layer architecture domain oriented and Microsoft SQL Server 2008.

•	Microsoft development technologies and security development trainer.Hardware support technician";


		public class Item
		{
			public string Name { get; set; }
		}

		[Fact]
		public void CanQuery()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Query<Item>()
						.Search(x => x.Name, query)
						.ToList();
				}
			}
		}
	}
}