// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Acornima;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Processor;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Exception = System.Exception;
using QueryParser = Raven.Server.Documents.Queries.Parser.QueryParser;

namespace Raven.Server.Documents.TcpHandlers
{
    public enum SubscriptionError
    {
        ConnectionRejected,
        Error
    }

    public sealed class SubscriptionOperationScope
    {
        public const string ConnectionPending = "ConnectionPending";
        public const string ConnectionActive = "ConnectionActive";
        public const string BatchSendDocuments = "BatchSendDocuments";
        public const string BatchWaitForAcknowledge = "BatchWaitForAcknowledge";
    }

    public class SubscriptionConnection : SubscriptionConnectionBase<DatabaseIncludesCommandImpl>
    {
        private static readonly ParserOptions DefaultParserOptions = ParserOptions.Default with { AllowReturnOutsideFunction = true };

        private readonly DocumentDatabase _database;

        public long CurrentBatchId;

        public SubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer bufferToCopy, string database)
            : base(tcpConnection.DocumentDatabase.SubscriptionStorage, tcpConnection, serverStore, bufferToCopy, tcpConnectionDisposable, database, tcpConnection.DocumentDatabase.DatabaseShutdown)
        {
            _database = tcpConnection.DocumentDatabase;
            CurrentBatchId = ISubscriptionConnection.NonExistentBatch;
        }

        protected SubscriptionConnectionsState State;

        public SubscriptionConnectionsState GetSubscriptionConnectionState()
        {
            var subscriptions = _database.SubscriptionStorage.Subscriptions;
            State = subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsState(_database.Name, subId, TcpConnection.DocumentDatabase.SubscriptionStorage));
            return State;
        }

        private SubscriptionPatchDocument SetupFilterAndProjectionScript()
        {
            SubscriptionPatchDocument patch = null;

            if (string.IsNullOrWhiteSpace(Subscription.Script) == false)
            {
                patch = new SubscriptionPatchDocument(Subscription.Script, Subscription.Functions);
            }
            return patch;
        }

        protected override void DisposeInternal()
        {
            Stats.LastConnectionStats.Complete();
            TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForConnectionEnded(this);

            base.DisposeInternal();
        }

        public struct ParsedSubscription
        {
            public string Collection;
            public string Script;
            public string[] Functions;
            public bool Revisions;
            public string[] Includes;
            public string[] CounterIncludes;
            internal TimeSeriesIncludesField TimeSeriesIncludes;
        }

        public static ParsedSubscription ParseSubscriptionQuery(string query)
        {
            var queryParser = new QueryParser();
            queryParser.Init(query);
            var q = queryParser.Parse();

            if (q.IsDistinct)
                throw new NotSupportedException("Subscription does not support distinct queries");
            if (q.From.Index)
                throw new NotSupportedException("Subscription must specify a collection to use");
            if (q.GroupBy != null)
                throw new NotSupportedException("Subscription cannot specify a group by clause");
            if (q.OrderBy != null)
                throw new NotSupportedException("Subscription cannot specify an order by clause");
            if (q.UpdateBody != null)
                throw new NotSupportedException("Subscription cannot specify an update clause");

            bool revisions = false;
            if (q.From.Filter is BinaryExpression filter)
            {
                switch (filter.Operator)
                {
                    case OperatorType.Equal:
                    case OperatorType.NotEqual:
                        if (!(filter.Left is FieldExpression fe) || fe.Compound.Count != 1)
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        if (string.Equals(fe.Compound[0].Value, "Revisions", StringComparison.OrdinalIgnoreCase) == false)
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        if (filter.Right is ValueExpression ve)
                        {
                            revisions = filter.Operator == OperatorType.Equal && ve.Value == ValueTokenType.True;
                            if (ve.Value != ValueTokenType.True && ve.Value != ValueTokenType.False)
                                throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        }
                        else
                        {
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        }
                        break;

                    default:
                        throw new NotSupportedException("Subscription must not specify a collection filter (move it to the where clause)");
                }
            }
            else if (q.From.Filter != null)
            {
                throw new NotSupportedException("Subscription must not specify a collection filter (move it to the where clause)");
            }

            List<string> includes = null;
            List<string> counterIncludes = null;
            TimeSeriesIncludesField timeSeriesIncludes = null;
            if (q.Include != null)
            {
                foreach (var include in q.Include)
                {
                    switch (include)
                    {
                        case MethodExpression me:
                            var includeType = QueryMethod.GetMethodType(me.Name.Value);
                            switch (includeType)
                            {
                                case MethodType.Counters:
                                    QueryValidator.ValidateIncludeCounter(me.Arguments, q.QueryText, null);

                                    counterIncludes ??= new List<string>();

                                    if (me.Arguments.Count > 0)
                                    {
                                        var argument = me.Arguments[0];

                                        counterIncludes.Add(ExtractPathFromExpression(argument, q));
                                    }
                                    break;
                                case MethodType.TimeSeries:
                                    QueryValidator.ValidateIncludeTimeseries(me.Arguments, q.QueryText, null);

                                    timeSeriesIncludes ??= new TimeSeriesIncludesField();

                                    switch (me.Arguments.Count)
                                    {
                                        case 1:
                                            {
                                                if (!(me.Arguments[0] is MethodExpression methodExpression))
                                                    throw new InvalidQueryException($"Expected to get include '{nameof(MethodType.TimeSeries)}' clause expression, but got: '{me.Arguments[0]}'.", q.QueryText);

                                                switch (methodExpression.Arguments.Count)
                                                {
                                                    case 1:
                                                        {
                                                            // include timeseries(last(11))
                                                            var (type, count) = TimeseriesIncludesHelper.ParseCount(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(Client.Constants.TimeSeries.All, type, count);
                                                            break;
                                                        }
                                                    case 2:
                                                        {
                                                            // include timeseries(last(600, 'seconds'))
                                                            var (type, time) = TimeseriesIncludesHelper.ParseTime(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(Client.Constants.TimeSeries.All, type, time);

                                                            break;
                                                        }
                                                    default:
                                                        throw new InvalidQueryException($"Got invalid arguments count '{methodExpression.Arguments.Count}' in '{methodExpression.Name}' method.", q.QueryText);
                                                }
                                            }
                                            break;
                                        case 2: // include timeseries('Name', last(7, 'months'));
                                            {
                                                if (!(me.Arguments[1] is MethodExpression methodExpression))
                                                    throw new InvalidQueryException($"Expected to get include {nameof(MethodType.TimeSeries)} clause expression, but got: {me.Arguments[1]}.", q.QueryText);

                                                string name = TimeseriesIncludesHelper.ExtractValueFromExpression(me.Arguments[0]);

                                                switch (methodExpression.Arguments.Count)
                                                {
                                                    case 1:
                                                        {
                                                            // last count query
                                                            var (type, count) = TimeseriesIncludesHelper.ParseCount(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(name, type, count);
                                                            break;
                                                        }
                                                    case 2:
                                                        {
                                                            // last time query
                                                            var (type, time) = TimeseriesIncludesHelper.ParseTime(methodExpression, q.QueryText);
                                                            timeSeriesIncludes.AddTimeSeries(name, type, time);
                                                            break;
                                                        }
                                                    default:
                                                        throw new InvalidQueryException($"Got invalid arguments count '{methodExpression.Arguments.Count}' in '{methodExpression.Name}' method.", q.QueryText);
                                                }
                                            }
                                            break;
                                        default:
                                            throw new NotSupportedException($"Invalid number of arguments '{me.Arguments.Count}' in include {nameof(MethodType.TimeSeries)} clause expression.");
                                    }
                                    break;
                                default:
                                    throw new NotSupportedException($"Subscription include expected to get {MethodType.Counters} or {nameof(MethodType.TimeSeries)} but got {includeType}");
                            }
                            break;
                        default:
                            includes ??= new List<string>();

                            includes.Add(ExtractPathFromExpression(include, q));
                            break;
                    }
                }

                static string ExtractPathFromExpression(QueryExpression expression, Query q)
                {
                    switch (expression)
                    {
                        case FieldExpression fe:
                            (string fieldPath, string _) = QueryMetadata.ParseExpressionPath(expression, fe.FieldValue, q.From.Alias);
                            return fieldPath;

                        case ValueExpression ve:
                            (string memberPath, string _) = QueryMetadata.ParseExpressionPath(expression, ve.Token.Value, q.From.Alias);
                            return memberPath;

                        default:
                            throw new InvalidOperationException("Subscription only support include of fields, but got: " + expression);
                    }
                }
            }

            var collectionName = q.From.From.FieldValue;
            if (q.Where == null && q.Select == null && q.SelectFunctionBody.FunctionText == null)
            {
                return new ParsedSubscription
                {
                    Collection = collectionName,
                    Revisions = revisions,
                    Includes = includes?.ToArray(),
                    CounterIncludes = counterIncludes?.ToArray(),
                    TimeSeriesIncludes = timeSeriesIncludes
                };
            }

            var writer = new StringWriter();

            if (q.From.Alias != null)
            {
                writer.Write("var ");
                writer.Write(q.From.Alias);
                writer.WriteLine(" = this;");
            }
            else if (q.Select != null || q.SelectFunctionBody.FunctionText != null || q.Load != null)
            {
                throw new InvalidOperationException("Cannot specify a select or load clauses without an alias on the query");
            }
            if (q.Load != null)
            {
                Debug.Assert(q.From.Alias != null);

                var fromAlias = q.From.Alias.Value;
                foreach (var tuple in q.Load)
                {
                    writer.Write("var ");
                    writer.Write(tuple.Alias);
                    writer.Write(" = loadPath(this,'");
                    var fieldExpression = ((FieldExpression)tuple.Expression);
                    if (fieldExpression.Compound[0] != fromAlias)
                        throw new InvalidOperationException("Load clause can only load paths starting from the from alias: " + fromAlias);
                    writer.Write(fieldExpression.FieldValueWithoutAlias);
                    writer.WriteLine("');");
                }
            }
            if (q.Where != null)
            {
                writer.Write("if (");
                new JavascriptCodeQueryVisitor(writer.GetStringBuilder(), q).VisitExpression(q.Where);
                writer.WriteLine(" )");
                writer.WriteLine("{");
            }

            if (q.SelectFunctionBody.FunctionText != null)
            {
                writer.Write(" return ");
                writer.Write(q.SelectFunctionBody.FunctionText);
                writer.WriteLine(";");
            }
            else if (q.Select != null)
            {
                if (q.Select.Count != 1 || q.Select[0].Expression is MethodExpression == false)
                    throw new NotSupportedException("Subscription select clause must specify an object literal");
                writer.WriteLine();
                writer.Write(" return ");
                new JavascriptCodeQueryVisitor(writer.GetStringBuilder(), q).VisitExpression(q.Select[0].Expression);
                writer.WriteLine(";");
            }
            else
            {
                writer.WriteLine(" return true;");
            }
            writer.WriteLine();

            if (q.Where != null)
                writer.WriteLine("}");

            var script = writer.GetStringBuilder().ToString();

            // verify that the JS code parses
            try
            {
                new Parser(DefaultParserOptions).ParseScript(script);
            }
            catch (Exception e)
            {
                throw new InvalidDataException("Unable to parse: " + script, e);
            }
            return new ParsedSubscription
            {
                Collection = collectionName,
                Revisions = revisions,
                Script = script,
                Functions = q.DeclaredFunctions?.Values?.Select(x => x.FunctionText).ToArray() ?? Array.Empty<string>(),
                Includes = includes?.ToArray(),
                CounterIncludes = counterIncludes?.ToArray()
            };
        }

        protected override async Task OnClientAckAsync(string clientReplyChangeVector)
        {
            _database.ForTestingPurposes?.Subscription_ActionToCallDuringWaitForAck?.Invoke();

            await Processor.AcknowledgeBatchAsync(CurrentBatchId, clientReplyChangeVector);
            await SendConfirmAsync(TcpConnection.DocumentDatabase.Time.GetUtcNow());
        }

        public override Task SendNoopAckAsync(bool force = false) => State.SendNoopAck(force);

        protected override bool FoundAboutMoreDocs()
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var p = (IDatabaseSubscriptionProcessor)Processor;
                var globalEtag = p.GetLastItemEtag(context, Subscription.Collection);
                return globalEtag > State.GetLastEtagSent();
            }
        }

        protected override SubscriptionConnectionInUse MarkInUse() => new(_database.DatabaseInUse(skipUsagesCount: false));

        protected override void AfterProcessorCreation()
        {
            var p = (IDatabaseSubscriptionProcessor)Processor;
            p.Patch = SetupFilterAndProjectionScript();
        }

        protected override void RaiseNotificationForBatchEnd(string name, SubscriptionBatchStatsAggregator last) => _database.SubscriptionStorage.RaiseNotificationForBatchEnded(name, last);

        protected override async Task<SubscriptionBatchStatus> TryRecordBatchAndUpdateStatusAsync(IChangeVectorOperationContext context, SubscriptionBatchResult result)
        {
            //Entire unsent batch could contain docs that have to be skipped, but we still want to update the etag in the cv
            LastSentChangeVectorInThisConnection = result.LastChangeVectorSentInThisBatch;
            CurrentBatchId = await Processor.TryRecordBatchAsync(result.LastChangeVectorSentInThisBatch);

            State.LastChangeVectorSent = ChangeVectorUtils.MergeVectors(
                State.LastChangeVectorSent,
                result.LastChangeVectorSentInThisBatch);

            if (result.CurrentBatch.Count == 0)
                return SubscriptionBatchStatus.EmptyBatch;

            return SubscriptionBatchStatus.DocumentsSent;
        }

        protected virtual StatusMessageDetails GetDefault()
        {
            return new StatusMessageDetails
            {
                DatabaseName = $"for database '{DatabaseName}'",
                ClientType = "'client worker'",
                SubscriptionType = "subscription"
            };
        }

        public override ISubscriptionProcessor<DatabaseIncludesCommandImpl> CreateProcessor(SubscriptionConnectionBase<DatabaseIncludesCommandImpl> connection)
        {
            if (connection is SubscriptionConnection subscriptionConnection)
            {
                var database = connection.TcpConnection.DocumentDatabase;
                var server = database.ServerStore;
                if (connection.Subscription.Revisions)
                {
                    return new RevisionsDatabaseSubscriptionProcessor(server, database, subscriptionConnection);
                }

                return new DocumentsDatabaseSubscriptionProcessor(server, database, subscriptionConnection);
            }

            throw new InvalidOperationException($"Expected to create a processor for '{nameof(SubscriptionConnection)}', but got: '{connection.GetType().Name}'.");
        }

        protected override void GatherIncludesForDocument(DatabaseIncludesCommandImpl includeDocuments, Document document)
        {
            includeDocuments?.GatherIncludesForDocument(document);
        }

        public override SubscriptionConnectionInfo CreateConnectionInfo()
        {
            return new SubscriptionConnectionInfo()
            {
                ClientUri = ClientUri,
                Query = State.Query,
                LatestChangeVector = State.LastChangeVectorSent,
                ConnectionException = ConnectionException,
                RecentSubscriptionStatuses = RecentSubscriptionStatuses.ToList(),
                Date = SystemTime.UtcNow,
                Strategy = Strategy,
                TcpConnectionStats = TcpConnection.GetConnectionStats(),
                LastConnectionStats = Stats.LastConnectionStats,
                LastBatchesStats = Stats.GetBatchesPerformanceStats,
                ArchivedDataProcessingBehavior = SubscriptionState.ArchivedDataProcessingBehavior
            };
        }

        protected override StatusMessageDetails GetStatusMessageDetails()
        {
            var message = GetDefault();
            message.DatabaseName = $"{message.DatabaseName} on '{ServerStore.NodeTag}'";
            message.ClientType = $"{message.ClientType} with IP '{ClientUri}'";
            message.SubscriptionType = $"{message.SubscriptionType} '{_options?.SubscriptionName}', id '{SubscriptionId}'";

            return message;
        }
    }

    public sealed class SubscriptionConnectionsDetails
    {
        public List<SubscriptionConnectionDetails> Results;
        public SubscriptionMode SubscriptionMode;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Results)] = new DynamicJsonArray(Results.Select(d => d.ToJson())),
                [nameof(SubscriptionMode)] = SubscriptionMode
            };
        }
    }

    public sealed class SubscriptionConnectionDetails
    {
        public string ClientUri { get; set; }
        public string WorkerId { get; set; }
        public SubscriptionOpeningStrategy? Strategy { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ClientUri)] = ClientUri,
                [nameof(WorkerId)] = WorkerId,
                [nameof(Strategy)] = Strategy
            };
        }
    }
}
