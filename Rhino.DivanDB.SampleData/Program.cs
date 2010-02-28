using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.DivanDB.SampleData;
using Rhino.DivanDB.SampleData.NwindDataSetTableAdapters;
using System.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Rhino.DivanDB.Extensions;
using Rhino.DivanDB;

namespace SampleData
{
    class Program
    {
        static void Main(string[] args)
        {
            string dbPath;
            if (args.Length != 1)
            {
                Console.WriteLine("Please enter the path to your database: ");
                dbPath = Console.ReadLine();
            }
            else
                dbPath = args[0];
            var db = new DocumentDatabase(dbPath);
            SampleDataCreator.InsertNorthwindData(db);
        }        
    }
}
