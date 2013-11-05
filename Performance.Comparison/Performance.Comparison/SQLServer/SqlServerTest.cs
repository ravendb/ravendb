// -----------------------------------------------------------------------
//  <copyright file="SqlServerTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Diagnostics;
using System.Linq;

namespace Performance.Comparison.SQLServer
{
	public class SqlServerTest : IStoragePerformanceTest
	{
		private readonly string connectionString;

		public SqlServerTest()
		{
			connectionString = @"Data Source=.\SQLExpress;Initial Catalog=VoronTests;Integrated Security=true";
		}

		public string StorageName { get { return "SQL Server";  } }

		private void NewDatabase()
		{
			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();


				using (var command = new SqlCommand("if object_id('Items') is not null  drop table items", connection))
				{
					command.ExecuteNonQuery();
				}
				using (var command = new SqlCommand("CREATE TABLE Items(Id INTEGER PRIMARY KEY, Value BINARY(128))", connection))
				{
					command.ExecuteNonQuery();
				}
			}
		}
		public List<PerformanceRecord> WriteSequential()
		{
			var sequentialIds = Enumerable.Range(0, Constants.ItemsPerTransaction * Constants.WriteTransactions);

			return Write(string.Format("[SQL Server] sequential write ({0} items)", Constants.ItemsPerTransaction), sequentialIds,
						 Constants.ItemsPerTransaction, Constants.WriteTransactions);
		}

		public List<PerformanceRecord> WriteRandom(HashSet<int> randomIds)
		{
			return Write(string.Format("[SQL Server] random write ({0} items)", Constants.ItemsPerTransaction), randomIds,
						 Constants.ItemsPerTransaction, Constants.WriteTransactions);
		}

		public PerformanceRecord ReadSequential()
		{
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

			return Read(string.Format("[SQL Server] sequential read ({0} items)", Constants.ReadItems), sequentialIds);
		}

		public PerformanceRecord ReadRandom(HashSet<int> randomIds)
		{
			return Read(string.Format("[SQL Server] random read ({0} items)", Constants.ReadItems), randomIds);
		}

		private List<PerformanceRecord> Write(string operation, IEnumerable<int> ids, int itemsPerTransaction, int numberOfTransactions)
		{
			NewDatabase();

			var records = new List<PerformanceRecord>();

			var value = new byte[128];
			new Random().NextBytes(value);

			var sw = new Stopwatch();

			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();

				var enumerator = ids.GetEnumerator();
				sw.Restart();
				for (var transactions = 0; transactions < numberOfTransactions; transactions++)
				{
					sw.Restart();
					using (var tx = connection.BeginTransaction())
					{
						for (var i = 0; i < itemsPerTransaction; i++)
						{
							enumerator.MoveNext();

							using (var command = new SqlCommand("INSERT INTO Items (Id, Value) VALUES (@id, @value)", connection))
							{
								command.Transaction = tx;
								command.Parameters.Add("@id", SqlDbType.Int, 4).Value = enumerator.Current;
								command.Parameters.Add("@value", SqlDbType.Binary, 128).Value = value;

								var affectedRows = command.ExecuteNonQuery();

								Debug.Assert(affectedRows == 1);
							}
						}
						
						tx.Commit();
					}
					sw.Stop();

					records.Add(new PerformanceRecord
					{
						Operation = operation,
						Time = DateTime.Now,
						Duration = sw.ElapsedMilliseconds,
						ProcessedItems = itemsPerTransaction
					});
				}

				sw.Stop();
			}

			return records;
		}

		private PerformanceRecord Read(string operation, IEnumerable<int> ids)
		{
			var buffer = new byte[128];

			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();

				var sw = Stopwatch.StartNew();
				var processed = 0;
				using (var tx = connection.BeginTransaction())
				{
					foreach (var id in ids)
					{

						using (var command = new SqlCommand("SELECT Value FROM Items WHERE ID = " + id, connection))
						{
							command.Transaction = tx;
							using (var reader = command.ExecuteReader())
							{
								while (reader.Read())
								{
									long bytesRead;
									long fieldOffset = 0;

									while ((bytesRead = reader.GetBytes(0, fieldOffset, buffer, 0, buffer.Length)) > 0)
									{
										fieldOffset += bytesRead;
									}
								}
							}
						}

						processed++;
					}
				}

				sw.Stop();

				return new PerformanceRecord
				{
					Operation = operation,
					Time = DateTime.Now,
					Duration = sw.ElapsedMilliseconds,
					ProcessedItems = processed
				};
			}
		}
	}
}