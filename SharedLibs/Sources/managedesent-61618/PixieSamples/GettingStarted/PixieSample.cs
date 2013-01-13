//-----------------------------------------------------------------------
// <copyright file="PixieSample.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Isam.Esent;

namespace PixieSample
{
    class PixieSample
    {
        private const string Database = @"test\sample.edb";

        static void CreateDatabase()
        {
            using (Connection connection = Esent.CreateDatabase(Database))
            using (Table table = connection.CreateTable("mytable"))
            {
                connection.UsingLazyTransaction(() =>
                {
                    table.CreateColumn(new ColumnDefinition("myid", ColumnType.Int32));
                    table.CreateColumn(new ColumnDefinition("mydata", ColumnType.Text));
                });
            }
        }

        static void InsertRecords()
        {
            using (Connection connection = Esent.OpenDatabase(Database))
            using (Table table = connection.OpenTable("mytable"))
            {
                connection.UsingLazyTransaction(() =>
                {
                    for (int i = 0; i < 10; ++i)
                    {
                        table.NewRecord()
                            .SetColumn("myid", i)
                            .SetColumn("mydata", "some data")
                            .Save();
                    }
                });
            }
        }

        static void UpdateRecords()
        {
            using (Connection connection = Esent.OpenDatabase(Database))
            using (Table table = connection.OpenTable("mytable"))
            {
                connection.UsingLazyTransaction(() =>
                {
                    Record[] records = table.ToArray();

                    records[0]["mydata"] = "some other data";
                    records[1]["mydata"] = "even different data";
                    records[2]["mydata"] = "this update will be lost";

                    records[0].Save();
                    records[1].Save();
                    // records[2] isn't saved so the update will be cancelled when the transaction commits
                });
            }
        }

        static void DeleteRecords()
        {
            using (Connection connection = Esent.OpenDatabase(Database))
            using (Table table = connection.OpenTable("mytable"))
            {
                IEnumerable<Record> recordsToDelete = from record in table
                                      where (0 == ((int)record["myid"] % 2))
                                      select record;

                connection.UsingLazyTransaction(() =>
                {
                    foreach (Record record in recordsToDelete)
                    {
                        record.Delete();
                    }
                });
            }
        }

        static void DumpRecords()
        {
            using (Connection connection = Esent.OpenDatabase(Database))
            using (Table table = connection.OpenTable("mytable"))
            {
                foreach (Record record in table)
                {
                    Console.WriteLine("{0} : {1}", record["myid"], record["mydata"]);
                }
            }
        }

        static void Main()
        {
            CreateDatabase();

            Console.WriteLine("Insert");
            InsertRecords();
            DumpRecords();

            Console.WriteLine();
            Console.WriteLine("Delete");
            DeleteRecords();
            DumpRecords();

            Console.WriteLine();
            Console.WriteLine("Update");
            UpdateRecords();
            DumpRecords();

            Console.WriteLine();
            Console.WriteLine("Execute SQL");
            SqlConnection sql = Esent.CreateSqlConnection();
            sql.Execute(String.Format("ATTACH DATABASE '{0}'", Database));
            sql.Execute("INSERT INTO mytable (myid, mydata) VALUES (101, 'this data inserted through SQL')");
            DumpRecords();
        }
    }
}
