using System;
using System.Linq.Expressions;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Core.ChangesApi;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
	public class Customer
	{
		public string Region;
		public string Id;
	}

	public class Invoice
	{
		public string Customer;
	}

	public class Program
	{
		private static void Main()
		{
            Expression<Func<object>> a = () => new string[0];
            
            Console.WriteLine(a.ToString());

            Console.WriteLine();

            var ab =
                ExpressionStringBuilder.ExpressionToString(new DocumentConvention(), false, typeof(object), "docs", a);
            Console.WriteLine(ab);
        }

	}
}