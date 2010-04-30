using System;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Client.Tests.Document;
using Raven.Database.Indexing;
using Raven.Database.Linq;

namespace Raven.Tryouts
{
	internal class Program
	{
	

		public static void Main()
		{
			BasicConfigurator.Configure(new ConsoleAppender
			{
				Layout = new SimpleLayout()
			});
			try
			{
				var dynamicViewCompiler = new DynamicViewCompiler("a", new IndexDefinition
				{
					Map = @"
from post in docs.Posts
where post.Published == 'aasds'
select new {post.PostedAt }
"
				});
				dynamicViewCompiler.GenerateInstance();
				Console.WriteLine(dynamicViewCompiler.CompiledQueryText);
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}

}