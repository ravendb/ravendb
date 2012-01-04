using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class BAM : LocalClientTest
	{
		[Fact]
		public void get_dbnames_test()
		{
			using (var server = GetNewServer())
			using (var docStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				var dbNames = docStore.DatabaseCommands.GetDatabaseNames();

				Assert.NotEmpty(dbNames);
			}
		}
	}
}
