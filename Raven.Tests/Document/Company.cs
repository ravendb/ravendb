//-----------------------------------------------------------------------
// <copyright file="Company.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Tests.Document
{
	public class Company
	{
		public decimal AccountsReceivable { get; set;}
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
}
