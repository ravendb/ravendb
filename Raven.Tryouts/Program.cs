using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.FileSystem;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Database.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.MailingList;
using Version = Lucene.Net.Util.Version;

namespace Raven.Tryouts
{
    public class Customer
    {
        public string Region;
        public string Id;
    }

    public class Invoice
    {
        public string Customer;
    }
    public class Program
    {
		static readonly HashSet<String> minimizeQueries = new HashSet<string>();  
        private static void Main()
        {           
            
            var seperator = new String('#', 80);

            // var query = @"( -Title:(RavenDB) AND Title:(*))";
            // parser.Parse(query);
			using (var reader = File.OpenText(@"c:\work\queries.txt"))
            using (var writer = File.CreateText(@"c:\work\error_queries.txt"))
            using (var defaultAnalyzer = new StandardAnalyzer(Version.LUCENE_29))
            using (var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzer))
            {
	            QueryBuilder.UseLuceneASTParser = false;
                reader.ReadLine();
                var luceneSW = new Stopwatch();
                var talSW = new Stopwatch();
                int queryCount = 0;
                var count = 0;
                do
                {
                    LuceneQueryParser parser = new LuceneQueryParser();
                    parser.LuceneAST = null;
                    var query = string.Empty;
                    var line = reader.ReadLine();
                    //for multi line queries
                    while (line != seperator && line != null)
                    {
                        query += line;
                        query += "\n";
                        line = reader.ReadLine();
                    }
                    queryCount++;
                    if (queryCount % 100 == 0) Console.WriteLine("Parsed {0} queries so far.", queryCount);
                    luceneSW.Start();
                    QueryBuilder.BuildQuery(query, perFieldAnalyzerWrapper);
                    luceneSW.Stop();
                    talSW.Start();
                    parser.Parse(query);
	                parser.LuceneAST.ToQuery(new LuceneASTQueryConfiguration() {Analayzer = perFieldAnalyzerWrapper, DefaultOperator = QueryOperator.Or, FieldName = string.Empty});
                    talSW.Stop();
                    if (parser.LuceneAST == null)
                    {
                        count++;
                        writer.WriteLine(seperator);
                        writer.WriteLine(query);
                    }
                } while (!reader.EndOfStream);
                writer.WriteLine(seperator);
                writer.WriteLine("Total of syntax error queries: {0}.", count);
                writer.WriteLine(seperator);
                Console.WriteLine("It took QueryBuilder {0}ms to parse through {1} queries.", luceneSW.ElapsedMilliseconds, queryCount);
                Console.WriteLine("It took GPPG  parser {0}ms to parse through {1} queries.", talSW.ElapsedMilliseconds, queryCount);
                Console.WriteLine("Total of syntax error queries: {0}.", count);
                Console.Read();	            
            }
        }

    }
}
