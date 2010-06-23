using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using System.Dynamic;
using System.Runtime.CompilerServices;
using Microsoft.CSharp.RuntimeBinder;
using Newtonsoft.Json.Linq;
using System.IO;
using Raven.Client.Document;

namespace Raven.Client.Tests.Document
{        
    public class DynamicDocuments
    {
        [Fact]
        public void Can_Store_and_Load_Dynamic_Documents()
        {                                          
            //When running in the XUnit GUI strange things happen is we just create a path relative to 
            //the .exe itself, so make our folder in the System temp folder instead ("<user>\AppData\Local\Temp")
            string directoryName =  Path.Combine(Path.GetTempPath(), "ravendb.RavenDynamicDocs");
            if (Directory.Exists(directoryName))
            {
                Directory.Delete(directoryName, true);
            }

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

            using (var db = new DocumentStore() { DataDirectory = directoryName })
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
                    
                    //Pull the docs back out of RavenDB and see if the values are the same
                    dynamic employeeLoad = session.Load<dynamic>(idEmployee);
                    Assert.Same("John Smith", employee.Name);
                    Assert.Same("0111 123123", employee.Phones.Home);
                    Assert.Same("0772 321123", employee.Phones.Office);
                    Assert.Contains(123.4M, employee.Prices);
                    Assert.Contains(123432.54M, employee.Prices);
                    Assert.Throws<RuntimeBinderException>(() => employee.Address);

                    dynamic personLoad = session.Load<CustomDynamicClass>(idPerson);
                    Assert.Same("Ellen", person.FirstName);
                    Assert.Same("Adams", person.LastName);
                    Assert.Throws<RuntimeBinderException>(() => person.Age);
                }
            }
        }
    }
}


