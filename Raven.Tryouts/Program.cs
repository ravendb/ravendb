using Raven.Tests.MailingList;

class Program
{
	static void Main(string[] args)
	{
		new FailingBulkInsertTest().CanBulkInsert();
	} 
}