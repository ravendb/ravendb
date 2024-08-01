using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.Server.Utils;

public sealed class ChangeVector
{
    internal static readonly PerCoreContainer<FastList<ChangeVector>> PerCoreChangeVectors = new PerCoreContainer<FastList<ChangeVector>>(32);

    private const char Separator = '|';
    private string _changeVector;

    private ChangeVector _order;
    public ChangeVector Order => _order ?? this;

    private ChangeVector _version;
    public ChangeVector Version => _version ?? this;

    public ChangeVector(string changeVector, IChangeVectorOperationContext context)
        : this(changeVector, throwOnRecursion: false, context)
    {
    }

    public ChangeVector(ChangeVector version, ChangeVector order)
    {
        _version = version;
        _order = order;
    }

    public ChangeVector(string changeVector, bool throwOnRecursion, IChangeVectorOperationContext context) => Renew(changeVector, throwOnRecursion, context);

    public void Renew(string changeVector, bool throwOnRecursion, IChangeVectorOperationContext context)
    {
        _order = null;
        _version = null;
        _changeVector = null;

        if (changeVector == null)
            return;

        if (changeVector.Contains(Separator))
        {
            if (throwOnRecursion)
                throw new ArgumentException("Recursion was detected");

            var parts = changeVector.Split(Separator);
            if (parts.Length != 2)
                throw new ArgumentException($"Invalid change vector {changeVector}");

            _order = context.GetChangeVector(parts[0], throwOnRecursion: true);
            _version = context.GetChangeVector(parts[1], throwOnRecursion: true);
            return;
        }

        _changeVector = changeVector;
    }

    public void Renew(ChangeVector version, ChangeVector order)
    {
        _order = order;
        _version = version;
        _changeVector = null;
    }

    public bool IsNullOrEmpty =>
        string.IsNullOrEmpty(_changeVector) &&
        string.IsNullOrEmpty(_order?._changeVector) &&
        string.IsNullOrEmpty(_version?._changeVector);

    public bool IsSingle => string.IsNullOrEmpty(_order?._changeVector) &&
                            string.IsNullOrEmpty(_version?._changeVector);

    public bool IsEqual(ChangeVector changeVector)
    {
        if (IsSingle && changeVector.IsSingle)
            return _changeVector == changeVector._changeVector;

        return Order.IsEqual(changeVector.Order) &&
               Version.IsEqual(changeVector.Version);
    }

    public bool Contains(string dbId)
    {
        if (IsSingle == false)
            throw new InvalidOperationException("Can't be performed on non-single change vector");
        if (dbId == null) 
            throw new ArgumentNullException(nameof(dbId));
        return _changeVector.Contains(dbId);
    }

    public ChangeVector MergeWith(ChangeVector changeVector, IChangeVectorOperationContext context) => Merge(this, changeVector, context);

    public ChangeVector MergeWith(string changeVector, IChangeVectorOperationContext context) => Merge(this, context.GetChangeVector(changeVector), context);

    public ChangeVector UpdateOrder(string nodeTag, string dbId, long etag, IChangeVectorOperationContext context)
    {
        var order = UpdateInternal(nodeTag, dbId, etag, Order, context);
        if (IsSingle || order == Order) // nothing changed
            return order;

        return context.GetChangeVector(Version, order);
    }

    public ChangeVector MergeOrderWith(ChangeVector changeVector, IChangeVectorOperationContext context)
    {
        var orderMerge = ChangeVectorUtils.MergeVectors(changeVector.Order._changeVector, Order._changeVector);
        if (IsSingle)
            return context.GetChangeVector(orderMerge);

        return context.GetChangeVector(Version, orderMerge);
    }

    public ChangeVector UpdateVersion(string nodeTag, string dbId, long etag, IChangeVectorOperationContext context)
    {
        var version = UpdateInternal(nodeTag, dbId, etag, Version, context);
        if (IsSingle || version == Version)
            return version; // nothing changed

        return context.GetChangeVector(version, Order);
    }

    public static ConflictStatus GetConflictStatusForDocument(ChangeVector remote, ChangeVector local) => GetConflictStatusInternal(remote?.Version, local?.Version);
    
    public static ConflictStatus GetConflictStatus(IChangeVectorOperationContext context, string remote, string local) => 
        GetConflictStatusInternal(context.GetChangeVector(remote)?.Version, context.GetChangeVector(local)?.Version);

    private ChangeVector UpdateInternal(string nodeTag, string dbId, long etag, ChangeVector changeVector, IChangeVectorOperationContext context)
    {
        EnsureValid();
        var result = ChangeVectorUtils.TryUpdateChangeVector(nodeTag, dbId, etag, changeVector);
        if (result.IsValid)
        {
            return context.GetChangeVector(result.ChangeVector);
        }

        return this;
    }

    private static ConflictStatus GetConflictStatusInternal(ChangeVector remote, ChangeVector local)
    {
        remote?.EnsureValid();
        local?.EnsureValid();

        return ChangeVectorUtils.GetConflictStatus(remote?.AsString(), local?.AsString());
    }

    public static ChangeVector Merge(ChangeVector cv1, ChangeVector cv2, IChangeVectorOperationContext context)
    {
        if (cv1 == null)
            return cv2;

        if (cv2 == null)
            return cv1;

        if (cv1.IsSingle && cv2.IsSingle)
        {
            var result = ChangeVectorUtils.MergeVectors(cv1._changeVector, cv2._changeVector);
            return context.GetChangeVector(result);
        }

        if (cv1.IsSingle == false && cv2.IsSingle == false)
        {
            var orderMerge = ChangeVectorUtils.MergeVectors(cv1.Order._changeVector, cv2.Order._changeVector);
            var versionMerge = ChangeVectorUtils.MergeVectors(cv1.Version._changeVector, cv2.Version._changeVector);
            return context.GetChangeVector(versionMerge, orderMerge);
        }

        // we are keeping the existing order without merging it with the version of the single change vector
        var mergedOrder = cv2.IsSingle ? cv1.Order : cv2.Order;
        var mergedVersion = ChangeVectorUtils.MergeVectors(cv1.Version._changeVector, cv2.Version._changeVector);
        return context.GetChangeVector(mergedVersion, mergedOrder);
    }

    public static ChangeVector MergeWithDatabaseChangeVector(DocumentsOperationContext context, ChangeVector changeVector)
    {
        var databaseChangeVector = context.LastDatabaseChangeVector ??= DocumentsStorage.GetDatabaseChangeVector(context);
        if (changeVector == null)
            return databaseChangeVector;

        return changeVector.MergeOrderWith(context.LastDatabaseChangeVector, context);
    }

    public static void MergeWithDatabaseChangeVector(DocumentsOperationContext context, string changeVector)
    {
        if (changeVector == null)
            return;

        MergeWithDatabaseChangeVector(context, context.GetChangeVector(changeVector));
    }

    public static ChangeVector MergeWithNewDatabaseChangeVector(DocumentsOperationContext context, ChangeVector changeVector, long? newEtag = null)
    {
        newEtag ??= context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
        var databaseChangeVector = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context, newEtag.Value);
        context.LastDatabaseChangeVector = databaseChangeVector;

        if (changeVector == null)
            return databaseChangeVector;

        if (changeVector.IsSingle == false)
        {
            var version = MergeWithDatabaseChangeVector(context, changeVector.Version);
            var order = changeVector.Order.MergeOrderWith(databaseChangeVector, context);
            return new ChangeVector(version, order);
        }

        return MergeWithDatabaseChangeVector(context, changeVector);
    }

    public static ChangeVector MergeWithNewDatabaseChangeVector(DocumentsOperationContext context, string changeVector)
    {
        return MergeWithNewDatabaseChangeVector(context, context.GetChangeVector(changeVector));
    }

    public static ChangeVector Merge(List<ChangeVector> changeVectors, IChangeVectorOperationContext context)
    {
        var result = changeVectors[0];
        for (int i = 1; i < changeVectors.Count; i++)
        {
            result = Merge(result, changeVectors[i], context);
        }

        return result;
    }

    public static int CompareVersion(ChangeVector changeVector1, ChangeVector changeVector2) => 
        string.CompareOrdinal(changeVector1.Version.AsString(), changeVector2.Version.AsString());

    public static int CompareVersion(ChangeVector changeVector1, string changeVector2, IChangeVectorOperationContext context) => 
        CompareVersion(changeVector1, context.GetChangeVector(changeVector2));

    public static int CompareVersion(string changeVector1, string changeVector2, IChangeVectorOperationContext context)
    {
        var cv1 = context.GetChangeVector(changeVector1);
        var cv2 = context.GetChangeVector(changeVector2);

        return CompareVersion(cv1, cv2);
    }

    public ChangeVector RemoveId(string id, IChangeVectorOperationContext context)
    {
        if (TryRemoveIds(new HashSet<string>(capacity: 1) { id }, context, out var result))
            return result;

        return this;
    }

    public bool TryRemoveIds(HashSet<string> ids, IChangeVectorOperationContext context, out ChangeVector changeVector)
    {
        changeVector = this;

        if (IsNullOrEmpty)
            return false;

        if (ids == null)
            return false;

        if (string.IsNullOrEmpty(_changeVector) == false)
        {
            if (TryRemoveIdsInternal(_changeVector, ids, out var newChangeVector))
            {
                changeVector = context.GetChangeVector(newChangeVector);
                return true;
            }

            return false;
        }

        var versionSuccess = TryRemoveIdsInternal(Version._changeVector, ids, out var newVersionChangeVector);
        var orderSuccess = TryRemoveIdsInternal(Order._changeVector, ids, out var newOrderChangeVector);

        if (versionSuccess || orderSuccess)
        {
            changeVector = context.GetChangeVector(newVersionChangeVector, newOrderChangeVector);
            return true;
        }

        return false;

        static bool TryRemoveIdsInternal(string changeVector, HashSet<string> ids, out string newChangeVector)
        {
            var entries = changeVector.ToChangeVectorList();
            if (entries.RemoveAll(x => ids.Contains(x.DbId)) > 0)
            {
                newChangeVector = entries.SerializeVector();
                return true;
            }

            newChangeVector = changeVector;
            return false;
        }
    }

    private ChangeVector StripTags(string tag, string exclude, IChangeVectorOperationContext context)
    {
        if (IsNullOrEmpty)
            return this;

        if (string.IsNullOrEmpty(_changeVector) == false)
            return context.GetChangeVector(StripTags(_changeVector, tag, exclude));

        return context.GetChangeVector(StripTags(Version._changeVector, tag, exclude), StripTags(Order._changeVector, tag, exclude));
    }

    public ChangeVector StripMoveTag(IChangeVectorOperationContext context) => StripTags(ChangeVectorParser.MoveTag, exclude: null, context);

    public static ChangeVector StripMoveTag(string changeVectorStr, IChangeVectorOperationContext context) => context.GetChangeVector(changeVectorStr).StripMoveTag(context);

    public ChangeVector StripTrxnTags(IChangeVectorOperationContext context) => StripTags(ChangeVectorParser.TrxnTag, exclude: null, context);

    public ChangeVector StripSinkTags(IChangeVectorOperationContext context) => StripTags(ChangeVectorParser.SinkTag, exclude: null, context);

    public ChangeVector StripSinkTags(string exclude, IChangeVectorOperationContext context) => StripTags(ChangeVectorParser.SinkTag, exclude, context);

    private static string StripTags(string from, string tag, string exclude)
    {
        if (from == null)
            return null;

        if (from.Contains(tag, StringComparison.OrdinalIgnoreCase) == false)
            return from;

        var newChangeVector = new List<ChangeVectorEntry>();
        var changeVectorList = from.ToChangeVectorList();
        var tagAsInt = ChangeVectorExtensions.FromBase26(tag);

        foreach (var entry in changeVectorList)
        {
            if (entry.NodeTag != tagAsInt ||
                exclude?.Contains(entry.DbId) == true)
            {
                newChangeVector.Add(entry);
            }
        }

        return newChangeVector.SerializeVector();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureValid()
    {
        if (_order == null && _version == null)
        {
            return;
        }

        if (_order != null && _version != null)
        {
            if (_changeVector != null)
                ThrowInvalidInnerChangeVector();

            return;
        }

        ThrowInvalidChangeVectorState();
    }

    [DoesNotReturn]
    private void ThrowInvalidChangeVectorState()
    {
        throw new InvalidOperationException($"order and version must be either filled or null (inner: {_changeVector}, version:{_version}, order:{_order})");
    }

    [DoesNotReturn]
    private void ThrowInvalidInnerChangeVector()
    {
        throw new InvalidOperationException($"inner is '{_changeVector}' while order or version are not empty");
    }

    public static implicit operator string(ChangeVector changeVector) => changeVector?.AsString();

    public string AsString() => ToString();

    public override string ToString()
    {
        EnsureValid();

        if (IsSingle)
            return _changeVector;

        return $"{_order._changeVector}{Separator}{_version._changeVector}";
    }
}


public enum ChangeVectorMode
{
    Version,
    Order
}
