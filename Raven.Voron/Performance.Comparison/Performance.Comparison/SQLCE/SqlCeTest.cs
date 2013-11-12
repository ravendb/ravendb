// -----------------------------------------------------------------------
//  <copyright file="SqlCeTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlServerCe;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Performance.Comparison.SQLCE
{
	public class SqlCeTest : StoragePerformanceTestBase
	{
		private readonly string connectionString;
		private readonly string dbFileName;

		public SqlCeTest(string path, byte[] buffer)
		    : base(buffer)
		{
			dbFileName = Path.Combine(path, "sqlce-perf-test.sdf");
			connectionString = string.Format("Data Source={0}", dbFileName);
		}

		public override string StorageName { get { return "SQL CE";  } }

		private void NewDatabase()
		{
			if (File.Exists(dbFileName))
				File.Delete(dbFileName);

			using (var engine = new SqlCeEngine(connectionString))
			{
				engine.CreateDatabase();
			}

			using (var connection = new SqlCeConnection(connectionString))
			{
				connection.Open();

				using (var command = new SqlCeCommand("CREATE TABLE Items(Id INTEGER PRIMARY KEY, Value BINARY(128))", connection))
				{
					command.ExecuteNonQuery();
				}
			}
		}
        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data)
		{
			return Write(string.Format("[SQL CE] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
						 Constants.ItemsPerTransaction, Constants.WriteTransactions);
		}

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data)
		{
			return Write(string.Format("[SQL CE] random write ({0} items)", Constants.ItemsPerTransaction), data,
						 Constants.ItemsPerTransaction, Constants.WriteTransactions);
		}

		public override PerformanceRecord ReadSequential()
		{
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

			return Read(string.Format("[SQL CE] sequential read ({0} items)", Constants.ReadItems), sequentialIds);
		}

		public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds)
		{
			return Read(string.Format("[SQL CE] random read ({0} items)", Constants.ReadItems), randomIds);
		}

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions)
        {
            byte[] valueToWrite = null;

			NewDatabase();

			var records = new List<PerformanceRecord>();

			var sw = new Stopwatch();

			using (var connection = new SqlCeConnection(connectionString))
			{
				connection.Open();

				var enumerator = data.GetEnumerator();
				sw.Restart();
				for (var transactions = 0; transactions < numberOfTransactions; transactions++)
				{
					sw.Restart();
					using (var tx = connection.BeginTransaction())
					{
						for (var i = 0; i < itemsPerTransaction; i++)
						{
							enumerator.MoveNext();

						    valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);

							using (var command = new SqlCeCommand("INSERT INTO Items (Id, Value) VALUES (@id, @value)", connection))
							{
								command.Parameters.Add("@id", SqlDbType.Int, 4).Value = enumerator.Current.Id;
                                command.Parameters.Add("@value", SqlDbType.Binary, 128).Value = valueToWrite;

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
						ProcessedItems = itemsPerTransaction,
                        Memory = GetMemory()
					});
				}

				sw.Stop();
			}

			return records;
		}

		private PerformanceRecord Read(string operation, IEnumerable<int> ids)
		{
			var buffer = new byte[128];

			using (var connection = new SqlCeConnection(connectionString))
			{
				connection.Open();

				var sw = Stopwatch.StartNew();
				var processed = 0;
				using (var tx = connection.BeginTransaction())
				{
					foreach (var id in ids)
					{

						using (var command = new SqlCeCommand("SELECT Value FROM Items WHERE ID = " + id, connection))
						{
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
					ProcessedItems = processed,
                    Memory = GetMemory()
				};
			}
		}
	}
}