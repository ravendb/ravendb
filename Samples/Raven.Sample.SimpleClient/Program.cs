//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client;
using Raven.Database.Indexing;

namespace Raven.Sample.SimpleClient
{
    class Program
    {
        static void Main()
        {
            using (var documentStore = new DocumentStore { Url = "http://ipv4.fiddler:8080" })
            {
                documentStore.Initialize();

               

            }
        }

        public class U2
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
