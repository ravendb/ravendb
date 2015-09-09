using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Storage.Voron.Impl;
using Raven.Tests.MailingList;

namespace ConsoleApplication4
{
    class Program
    {
        public class Item
        {
            public int Number;
        }
        private static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "r1"
            }.Initialize())
            {
                for (int i = 0; i < 10; i++)
                {
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        while (true)
                        {
                            using (var session = store.OpenSession())
                            {
                                session.Load<dynamic>("users/1");
                            }
                        }
                    });
                }

                Console.ReadLine();
            }
        }

    }

    public class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }

        public string Phone { get; set; }
        public string Fax { get; set; }
    }
}
