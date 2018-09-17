//-----------------------------------------------------------------------
// <copyright file="DocumentBatch.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class DocumentBatch : AbstractRequestResponder
	{
	    private readonly static ILog log = LogManager.GetCurrentClassLogger();

        public override string UrlPattern
		{
			get { return @"^/bulk_docs(/(.+))?"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST", "PATCH", "EVAL", "DELETE" }; }
		}

		public override void Respond(IHttpContext context)
		{
			using (var cts = new CancellationTokenSource())
			{
				var timeout = cts.TimeoutAfter(Settings.DatbaseOperationTimeout);
				var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(context), cts.Token, timeout);
				
				switch (context.Request.HttpMethod)
				{
					case "POST":
						Batch(context);
						break;
					case "DELETE":
						OnBulkOperation(context, databaseBulkOperations.DeleteByIndex);
						break;
					case "PATCH":

						RavenJArray patchRequestJson;
						try
						{
						    long _;
                            patchRequestJson = context.ReadJsonArray(out _);
						}
						catch (InvalidOperationException e)
						{
							context.SetSerializationException(e);
							return;
						}
						catch (InvalidDataException e)
						{
							context.SetSerializationException(e);
							return;
						}

						var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
						OnBulkOperation(context, (index, query, allowStale) =>
							databaseBulkOperations.UpdateByIndex(index, query, patchRequests, allowStale));
						break;
					case "EVAL":
						RavenJObject advPatchRequestJson;
						try
						{
							advPatchRequestJson = context.ReadJsonObject<RavenJObject>();
						}
						catch (InvalidOperationException e)
						{
							context.SetSerializationException(e);
							return;
						}
						catch (InvalidDataException e)
						{
							context.SetSerializationException(e);
							return;
						}

						var advPatch = ScriptedPatchRequest.FromJson(advPatchRequestJson);
						OnBulkOperation(context, (index, query, allowStale) =>
							databaseBulkOperations.UpdateByIndex(index, query, advPatch, allowStale));
						break;
				}
			}
		}

		private void OnBulkOperation(IHttpContext context, Func<string, IndexQuery, bool, RavenJArray> batchOperation)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[2].Value;
			if (string.IsNullOrEmpty(index))
			{
				context.SetStatusToBadRequest();
				return;
			}
			var allowStale = context.GetAllowStale();
			var indexQuery = context.GetIndexQueryFromHttpContext(maxPageSize: int.MaxValue);

		    var status = new BulkOperationStatus();
			var sp = Stopwatch.StartNew();
			long id = 0;

			var task = Task.Factory.StartNew(() =>
			{
				var array = batchOperation(index, indexQuery, allowStale);
			    status.State = array;
				status.Completed = true;

				context.Log(log => log.Debug("\tBatch Operation worked on {0:#,#;;0} documents in {1}, task #: {2}", array.Length, sp.Elapsed, id));
			});

			Database.AddTask(task, status, out id);

			context.WriteJson(new
			{
				OperationId = id
			});
		}

	    public class BulkOperationStatus
	    {
            public RavenJArray State { get; set; }
            public bool Completed { get; set; }
	    }

	    private static long numberOfConcurrentBulkPosts = 0;

        private void Batch(IHttpContext context)
		{
		    var isDebugEnabled = log.IsDebugEnabled;

		    try
		    {
		        RavenJArray jsonCommandArray;

		        Stopwatch sp = null;
		        if (isDebugEnabled)
		        {
		            Interlocked.Increment(ref numberOfConcurrentBulkPosts);
		            sp = Stopwatch.StartNew();
		        }

		        long jsonCommandsSize;
		        try
		        {
		            jsonCommandArray = context.ReadJsonArray(out jsonCommandsSize);

		            if (isDebugEnabled)
		            {
		                sp.Stop();
		            }
		        }
		        catch (InvalidOperationException e)
		        {
		            context.SetSerializationException(e);
		            return;
		        }
		        catch (InvalidDataException e)
		        {
		            context.SetSerializationException(e);
		            return;
		        }

		        var transactionInformation = GetRequestTransaction(context);
		        var commands = (from RavenJObject jsonCommand in jsonCommandArray
		                select CommandDataFactory.CreateCommand(jsonCommand, transactionInformation))
		            .ToArray();

		        if (isDebugEnabled)
		        {
		            log.Debug(() =>
		            {
		                var baseMessage = string.Format(
		                    "\tRead bulk_docs data, {0:#,#;;0} commands, size: {1:#,#;;0} bytes, took: {2:#,#;;0}ms",
		                    commands.Length, jsonCommandsSize, sp.ElapsedMilliseconds);

		                if (commands.Length > 15
		                ) // this is probably an import method, we will input minimal information, to avoid filling up the log
		                {
		                    return baseMessage + Environment.NewLine + "\tExecuting "
		                           + string.Join(
		                               ", ",
		                               commands.GroupBy(x => x.Method).Select(x =>
		                                   string.Format("{0:#,#;;0} {1} operations", x.Count(), x.Key))) + "" +
		                           string.Format(", number of concurrent BulkPost: {0:#,#;;0}",
		                               Interlocked.Read(ref numberOfConcurrentBulkPosts));
		                }

		                var sb = new StringBuilder();
		                sb.AppendFormat("{0}", baseMessage);
		                foreach (var commandData in commands)
		                {
		                    sb.AppendFormat("\t{0} {1}{2}", commandData.Method, commandData.Key, Environment.NewLine);
		                }

		                return sb.ToString();
		            });

		            sp.Restart();
		        }

		        var batchResult = Database.Batch(commands);

		        if (isDebugEnabled)
		        {
		            log.Debug(string.Format(
		                "Executed {0:#,#;;0} operations, took: {1:#,#;;0}ms, number of concurrent BulkPost: {2:#,#;;0}",
		                commands.Length, sp.ElapsedMilliseconds, numberOfConcurrentBulkPosts));
		        }

		        context.WriteJson(batchResult);
		    }
		    finally
            {
                if (isDebugEnabled)
                    Interlocked.Decrement(ref numberOfConcurrentBulkPosts);
            }
		}
	}
}
