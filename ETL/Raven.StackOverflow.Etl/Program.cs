//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Server;
using Raven.StackOverflow.Etl.Posts;
using Raven.StackOverflow.Etl.Users;
using Rhino.Etl.Core;
using System.Linq;

namespace Raven.StackOverflow.Etl
{
	class Program
	{
	    public static readonly ConcurrentBag<Tuple<string, TimeSpan>> durations = new ConcurrentBag<Tuple<string, TimeSpan>>();

		static void Main(string[] args)
		{
            var commands = new ICommand[] {
		        new XMLToFileCommand(),
		        new FileToRavenCommand()
		        };

            ICommand selectedCommand = null;

		    try
		    {
                if (args.Count() == 0)
                    throw new ArgumentException("");

                foreach (var command in commands)
                {
                    if (command.CommandText.Equals(args[0], StringComparison.InvariantCultureIgnoreCase))
                    {
                        selectedCommand = command;
                        break;
                    }
                }

                if (selectedCommand== null)
                    throw new Exception("");

                selectedCommand.LoadArgs(args.Skip(1));
            }

		    catch (Exception e)
		    {
                if (!String.IsNullOrEmpty(e.Message))
                {
                    Console.WriteLine();
                    Console.WriteLine("Error message: " + e.Message);
                }

                Console.WriteLine();
                Console.WriteLine("Expected parameters:");
                foreach(var command in commands)
                {
                    command.WriteHelp(Console.Out);
                }
    	        
		        throw;
		    }

            
            ServicePointManager.DefaultConnectionLimit = int.MaxValue;

			Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
			Trace.WriteLine("Starting...");
			var sp = Stopwatch.StartNew();

            selectedCommand.Run();

			Console.WriteLine("Total execution time {0}", sp.Elapsed);
		}

	    private static void WaitForIndexingToComplete(DocumentDatabase documentDatabase)
		{
			Console.WriteLine("Waiting for indexing to complete");
			var sp2 = Stopwatch.StartNew();
			while (documentDatabase.HasTasks)
			{
				documentDatabase.TransactionalStorage.Batch(actions =>
				{
					var indexesStat = actions.GetIndexesStats().First();
					Console.WriteLine("{0} - {1:#,#} - {2:#,#} - {3}", indexesStat.Name,
						indexesStat.IndexingSuccesses,
						actions.GetDocumentsCount(),
						sp2.Elapsed);

					actions.Commit(CommitTransactionGrbit.LazyFlush);
				});

				Thread.Sleep(1000);
			}
		}

	}
}
