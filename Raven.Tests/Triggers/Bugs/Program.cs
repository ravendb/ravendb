using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using System.Collections.Specialized;
using Raven.Tests.Triggers.Bugs;

namespace RavenTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var store = new DocumentStore() { Url = "http://localhost:8080" };
            store.Initialize();

            //using (var session = store.OpenSession())
            //{
            //    session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1081");

            //    var person = session.Load<Person>("person/1");

            //    Console.WriteLine(session.Advanced.GetMetadataFor<Person>(person).Value<DateTime>("Last-Modified"));
            //    //person.Age = 25;
            //    //session.SaveChanges();
            //}


            using (var session = store.OpenSession())
            {
                session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1085");

                var person = new Person();
                person.Id = "person/1";
                person.FirstName = "Nabil";
                person.LastName = "Shuhaiber";
                person.Age = 31;
                person.Title = "Vice President";

                session.Store(person);
                session.SaveChanges();

                Console.WriteLine(session.Advanced.GetMetadataFor<Person>(person).Value<string>("CreatedDate"));
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1081");

                var person = session.Load<Person>("person/1");
                person.Age = 25;
                session.SaveChanges();

                Console.WriteLine(session.Advanced.GetMetadataFor<Person>(person).Value<string>("CreatedDate"));
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.DatabaseCommands.OperationsHeaders.Add("CurrentUserPersonId", "1022");

                var person = session.Load<Person>("person/1");

                person.FirstName = "Steve";
                person.LastName = "Richmond";
                session.SaveChanges();

                Console.WriteLine(session.Advanced.GetMetadataFor<Person>(person).Value<string>("CreatedDate"));
            }

            Console.ReadLine();
        }
    }
}
