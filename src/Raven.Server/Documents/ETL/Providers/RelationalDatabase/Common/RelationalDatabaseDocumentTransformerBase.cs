using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Jint.Native;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;

internal abstract class RelationalDatabaseDocumentTransformerBase<TRelationalConnectionString, TRelationalEtlConfiguration>  : EtlTransformer<RelationalDatabaseItem, RelationalDatabaseTableWithRecords, EtlStatsScope, EtlPerformanceOperation>
where TRelationalConnectionString: ConnectionString
where TRelationalEtlConfiguration: EtlConfiguration<TRelationalConnectionString>
{
    private readonly Transformation _transformation;
    protected readonly TRelationalEtlConfiguration Config;
    private readonly Dictionary<string, RelationalDatabaseTableWithRecords> _tables;
    private readonly Dictionary<string, Queue<Attachment>> _loadedAttachments;
    private readonly List<RelationalDatabaseTableWithRecords> _tablesForScript;
    private readonly List<RelationalDatabaseTableWithRecords> _etlTablesWithRecords;

    private EtlStatsScope _stats;

    protected RelationalDatabaseDocumentTransformerBase(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, TRelationalEtlConfiguration config, PatchRequestType patchRequestType)
        : base(database, context, new PatchRequest(transformation.Script, patchRequestType), null)
    {
        _transformation = transformation;
        Config = config;

        var destinationTables = transformation.GetCollectionsFromScript();

        LoadToDestinations = destinationTables;

        _tables = new Dictionary<string, RelationalDatabaseTableWithRecords>(destinationTables.Length, StringComparer.OrdinalIgnoreCase);
        _tablesForScript = new List<RelationalDatabaseTableWithRecords>(destinationTables.Length);


        _etlTablesWithRecords = GetEtlTables();
        // ReSharper disable once ForCanBeConvertedToForeach
        for (var i = 0; i < _etlTablesWithRecords.Count; i++)
        {
            var table = _etlTablesWithRecords[i];

            if (destinationTables.Contains(table.TableName, StringComparer.OrdinalIgnoreCase))
                _tablesForScript.Add(table);
        }

        if (_transformation.IsLoadingAttachments)
           _loadedAttachments = new Dictionary<string, Queue<Attachment>>(StringComparer.OrdinalIgnoreCase);
    }

    protected abstract List<RelationalDatabaseTableWithRecords> GetEtlTables();
    
    protected override string[] LoadToDestinations { get; }

    protected override void LoadToFunction(string tableName, ScriptRunnerResult cols)
    {
        if (tableName == null)
            ThrowLoadParameterIsMandatory(nameof(tableName));

        var result = cols.TranslateToObject(Context);
        var columns = new List<RelationalDatabaseColumn>(result.Count);
        var prop = new BlittableJsonReaderObject.PropertyDetails();

        for (var i = 0; i < result.Count; i++)
        {
            result.GetPropertyByIndex(i, ref prop);

            var relationalColumn = new RelationalDatabaseColumn { Id = prop.Name, Type = prop.Token, Value = prop.Value };

            if (_transformation.IsLoadingAttachments && 
                prop.Token == BlittableJsonToken.String && IsLoadAttachment(prop.Value as LazyStringValue, out var attachmentName))
            {
                var attachment = _loadedAttachments[attachmentName].Dequeue();

                relationalColumn.Type = 0;
                relationalColumn.Value = attachment.Stream;

                _stats.IncrementBatchSize(attachment.Stream.Length);
            }

            columns.Add(relationalColumn);
        }

        var newItem = new RelationalDatabaseItem(Current);
        newItem.Columns = columns;
        
        GetOrAdd(tableName).Inserts.Add(newItem);
        
        _stats.IncrementBatchSize(result.Size);
    }

    private static unsafe bool IsLoadAttachment(LazyStringValue value, out string attachmentName)
    {
        if (value.Length <= Transformation.AttachmentMarker.Length)
        {
            attachmentName = null;
            return false;
        }

        var buffer = value.Buffer;

        if (*(long*)buffer != 7883660417928814884 || // $attachm
            *(int*)(buffer + 8) != 796159589) // ent/
        {
            attachmentName = null;
            return false;
        }

        attachmentName = value;
        return true;
    }

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        var strReference = reference.ToString();
        if (_loadedAttachments.TryGetValue(strReference, out var loadedAttachments) == false)
        {
            loadedAttachments = new Queue<Attachment>();
            _loadedAttachments.Add(strReference, loadedAttachments);
        }

        loadedAttachments.Enqueue(attachment);
    }
    
    protected override void AddLoadedCounter(JsValue reference, string name, long value)
    {
        throw new NotSupportedException($"Counters aren't supported by {Config.EtlType.ToString()} ETL");
    }

    protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
    {
        throw new NotSupportedException($"Time series aren't supported by {Config.EtlType.ToString()} ETL");
    }

    private RelationalDatabaseTableWithRecords GetOrAdd(string tableName)
    {
        if (_tables.TryGetValue(tableName, out RelationalDatabaseTableWithRecords table) == false)
        {
            var relationalEtlTable = _etlTablesWithRecords.Find(x => x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));

            if (relationalEtlTable == null)
                ThrowTableNotDefinedInConfig(tableName);
            
            table = new RelationalDatabaseTableWithRecords(relationalEtlTable);
            _tables[tableName] = table;
            return table;
        }

        return table;
    }

    [DoesNotReturn]
    private void ThrowTableNotDefinedInConfig(string tableName)
    {
        throw new InvalidOperationException($"Table '{tableName}' was not defined in the configuration of {Config.EtlType.ToString()} ETL task");
    }

    public override IEnumerable<RelationalDatabaseTableWithRecords> GetTransformedResults()
    {
        return _tables.Values;
    }

    public override void Transform(RelationalDatabaseItem item, EtlStatsScope stats, EtlProcessState state)
    {
        _stats = stats;

        if (item.IsDelete == false)
        {
            Current = item;

            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
        }

        // ReSharper disable once ForCanBeConvertedToForeach
        for (int i = 0; i < _tablesForScript.Count; i++)
        {
            // delete all the rows that might already exist there

            var serverSideRelationalEtlTable = _tablesForScript[i];

            if (serverSideRelationalEtlTable.InsertOnlyMode)
                continue;

            GetOrAdd(serverSideRelationalEtlTable.TableName).Deletes.Add(item);
        }
    }
}
