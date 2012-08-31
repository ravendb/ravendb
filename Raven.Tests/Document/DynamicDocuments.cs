//-----------------------------------------------------------------------
// <copyright file="DynamicDocuments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Abstractions.Linq;
using Raven.Client.Embedded;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Xunit;
using System.Dynamic;
using System.IO;

namespace Raven.Tests.Document
{        
	public class DynamicDocuments
	{
		[Fact]
		public void Can_Store_and_Load_Dynamic_Documents()
		{                                          
			//When running in the XUnit GUI strange things happen is we just create a path relative to 
			//the .exe itself, so make our folder in the System temp folder instead ("<user>\AppData\Local\Temp")
			string directoryName =  Path.Combine(Path.GetTempPath(), "ravendb.RavenDynamicDocs");
			IOExtensions.DeleteDirectory(directoryName);
			dynamic person = new CustomDynamicClass();
			person.FirstName = "Ellen";
			person.LastName = "Adams";

			dynamic employee = new ExpandoObject();
			employee.Name = "John Smith";
			employee.Age = 33;
			employee.Phones = new ExpandoObject();
			employee.Phones.Home = "0111 123123";
			employee.Phones.Office = "0772 321123";
			employee.Prices = new List<decimal>() { 123.4M, 123432.54M };

			using (var db = new EmbeddableDocumentStore() { DataDirectory = directoryName })
			{
				db.Initialize();
				
				using (var session = db.OpenSession())
				{
					session.Store(employee);
					string idEmployee = employee.Id;
										
					session.Store(person);
					string idPerson = person.Id;

					//Check that a field called "Id" is added to the dynamic object (as it doesn't already exist)
					//and that it has something in it (not null and not empty)
					Assert.False(String.IsNullOrEmpty(idEmployee));
					Assert.False(String.IsNullOrEmpty(idPerson));

					session.SaveChanges();
					session.Advanced.Clear();
					//Pull the docs back out of RavenDB and see if the values are the same
					dynamic employeeLoad = session.Load<object>(idEmployee);
					Assert.Equal("John Smith", employeeLoad.Name);
					Assert.Equal("0111 123123", employeeLoad.Phones.Home);
					Assert.Equal("0772 321123", employeeLoad.Phones.Office);
					Assert.Contains(123.4m, employeeLoad.Prices);
					Assert.Contains(123432.54m, employeeLoad.Prices);
					Assert.IsType<DynamicNullObject>(employeeLoad.Address);

					dynamic personLoad = session.Load<object>(idPerson);
					Assert.Equal("Ellen", personLoad.FirstName);
					Assert.Equal("Adams", personLoad.LastName);
					Assert.Throws<RuntimeBinderException>(() => personLoad.Age);

				}
			}
		}
	}
}


