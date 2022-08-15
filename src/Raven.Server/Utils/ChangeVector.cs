using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;

namespace Raven.Server.Utils;

public class ChangeVector
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

    public ChangeVector UpdateVersion(string nodeTag, string dbId, long etag, IChangeVectorOperationContext context)
    {
        var version = UpdateInternal(nodeTag, dbId, etag, Version, context);
        if (IsSingle || version == Version)
            return version; // nothing changed

        return context.GetChangeVector(version, Order);
    }

    public static ConflictStatus GetConflictStatusForDocument(ChangeVector documentVector1, ChangeVector documentVector2) => GetConflictStatusInternal(documentVector1.Version, documentVector2.Version);

    public static ConflictStatus GetConflictStatusForDatabase(ChangeVector documentVector, ChangeVector databaseVector) => GetConflictStatusInternal(documentVector.Order, databaseVector.Order);

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
        remote.EnsureValid();
        local.EnsureValid();

        return ChangeVectorUtils.GetConflictStatus(remote.AsString(), local.AsString());
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

        var orderMerge = ChangeVectorUtils.MergeVectors(cv1.Order._changeVector, cv2.Order._changeVector);
        var versionMerge = ChangeVectorUtils.MergeVectors(cv1.Version._changeVector, cv2.Version._changeVector);
        return context.GetChangeVector(versionMerge, orderMerge);
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

    public ChangeVector StripTrxnTags(IChangeVectorOperationContext context) => StripTags(ChangeVectorParser.TrxnTag, exclude: null, context);

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
            /*if (_changeVector == null)
                    ThrowEmptyChangeVector();*/

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

    private void ThrowInvalidChangeVectorState()
    {
        throw new InvalidOperationException($"order and version must be either filled or null (inner: {_changeVector}, version:{_version}, order:{_order})");
    }

    private void ThrowInvalidInnerChangeVector()
    {
        throw new InvalidOperationException($"inner is '{_changeVector}' while order or version are not empty");
    }

    private static void ThrowEmptyChangeVector()
    {
        throw new InvalidOperationException("Empty change vector");
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
