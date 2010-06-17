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
        public void Can_store_dynamic_documents()
        {
            //see http://stackoverflow.com/questions/2634858/how-do-i-reflect-over-the-members-of-dynamic-object
            // http://blogs.msdn.com/b/csharpfaq/archive/2009/10/01/dynamic-in-c-4-0-introducing-the-expandoobject.aspx
            // http://msdn.microsoft.com/en-us/library/system.dynamic.expandoobject%28v=VS.100%29.aspx 
            //  http://stackoverflow.com/questions/1110481/handling-a-c-method-that-isnt-defined-on-a-dynamic-object-aka-respond-to-metho
            
            //LOOK at this : http://stackoverflow.com/questions/2079870/dynamically-adding-members-to-a-dynamic-object has interesting idea!!!
            //See if this can be converted to do the opposite, i.e. find all the members??
            object x = new ExpandoObject();
            var site = CallSite<Func<CallSite, object, object, object>>.Create(
                        Binder.SetMember(
                            Microsoft.CSharp.RuntimeBinder.CSharpBinderFlags.None,
                            "Foo",
                            null,
                            new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) }
                        )
                    );            
            site.Target(site, x, 42);            
            Console.WriteLine(((dynamic)x).Foo);                      

            // Creating a dynamic dictionary.
            dynamic person = new CustomDynamicClass();

            // Adding new dynamic properties. 
            // The TrySetMember method is called.
            //person.FirstName = "Ellen";
            //person.LastName = "Adams";

            //Our simple implementation means we can't get a value we haven't already added
            //Console.WriteLine(person.Blah);
            //Console.WriteLine("FirstName = {0}", person.FirstName);

            //Console.WriteLine("{0} is IDynamicMetaObjectProvider - {1}", person, person is IDynamicMetaObjectProvider);

            //This only work is the class that inherist from DynamicObject has overriden GetDynamicMemeberNames()
            //otherwise the method is called on the base (DynamicObject) and it returns an empty IEnumerable
            foreach (var property in ((DynamicObject)person).GetDynamicMemberNames())
            {
                Console.WriteLine(property);
            }

            dynamic employee = new ExpandoObject();
            employee.Name = "John Smith";
            employee.Age = 33;
            employee.Phones = new ExpandoObject();
            employee.Phones.Home = "0111 123123";
            employee.Phones.Office = "0772 321123";
            employee.Tags = new List<dynamic>() { 123.4D, 123432.54D };

             //Class inheriting from DynamicObject do below
            ((DynamicObject)person).GetDynamicMemberNames();

            //For ExpandoObject() do
            //var temp = (IDictionary<String, Object>)employee;
            //JObject json = new JObject();
            //foreach (var item in (IDictionary<String, Object>)employee)
            //{
            //    json.Add(item.Key, JToken.FromObject(item.Value));
            //}
            //Console.WriteLine(json.ToString());

            Console.WriteLine(JToken.FromObject(employee).ToString());

            Console.WriteLine(JToken.FromObject(person).ToString());

            //For everything else?? Maybe see http://dlr.codeplex.com/wikipage?title=Docs%20and%20specs&referringTitle=Home

            //When running in the XUnit GUI strange things happen is we just create a path relative to 
            //the .exe itself, so make our folder in the System temp folder instead ("<user>\AppData\Local\Temp")
            string directoryName =  Path.Combine(Path.GetTempPath(), "ravendb.RavenDynamicDocs");
            if (Directory.Exists(directoryName))
            {
                Directory.Delete(directoryName, true);
            }           

            using (var db = new DocumentStore() { DataDirectory = directoryName })
            {
                db.Initialize();
                
                using (var session = db.OpenSession())
                {
                    var idAnom = session.Store(new { Name = "Matt", Age = 19 });

                    //See http://stackoverflow.com/questions/1723875/can-method-parameters-be-dynamic-in-c
                    //and http://blogs.msdn.com/b/cburrows/archive/2008/11/14/c-dynamic-part-vi.aspx
                    //and http://msdn.microsoft.com/en-us/library/dd264736.aspx for an explanation, 
                    //basicially with dynamic, resolution takes place as run-time
                    var test = session.StoreDynamic(employee);
                    var idEmployee = session.Store((object)employee);
                    var idPerson = session.Store((object)person);
                    session.SaveChanges();

                    Console.WriteLine("Stored as {0} {1}, {2} {3} and {4}", idEmployee, employee.Id, idPerson, person.Id, idAnom);

                    var anomLoad = session.Load<object>(idAnom);
                    object employeeLoad = session.Load<dynamic>(idEmployee);
                    object personLoad = session.Load<dynamic>(idPerson);
                    Console.WriteLine(employeeLoad);
                }

            }
        }
    }
}

