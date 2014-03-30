using System.Collections.Generic;

namespace Raven.Tests.Core
{
	public class Company
	{
		public decimal AccountsReceivable { get; set; }
		public string Id { get; set; }
		public string Name { get; set; }
		public string Address1 { get; set; }
		public string Address2 { get; set; }
		public string Address3 { get; set; }
		public List<Contact> Contacts { get; set; }
		public int Phone { get; set; }
		public CompanyType Type { get; set; }

		public enum CompanyType
		{
			Public,
			Private
		}
	}

	public class Contact
	{
		public string Id { get; set; }
		public string FirstName { get; set; }
		public string Surname { get; set; }
		public string Email { get; set; }
	}
}