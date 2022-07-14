using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Replication;

namespace Raven.Server.Utils;

public class ChangeVector
{
    private const char Separator = '|';
    private string _changeVector;
        
    private ChangeVector _order;
    public ChangeVector Order => _order ?? this;

    private ChangeVector _version;
    public ChangeVector Version => _version ?? this;

    public ChangeVector(string changeVector) : this(changeVector, throwOnRecursion: false)
    {
    }

    public ChangeVector(string version, string order)
    {
        _version = new ChangeVector(version);
        _order = new ChangeVector(order);
    }

    private ChangeVector(string changeVector, bool throwOnRecursion) => Renew(changeVector, throwOnRecursion);

    private void Renew(string changeVector, bool throwOnRecursion)
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

            _order = new ChangeVector(parts[0], throwOnRecursion: true);
            _version = new ChangeVector(parts[1], throwOnRecursion: true);
            return;
        }

        _changeVector = changeVector;
    }

    public void Renew(string changeVector) => Renew(changeVector, throwOnRecursion: false);

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

    public ChangeVector MergeWith(ChangeVector changeVector) => Merge(this, changeVector);

    public ChangeVector MergeWith(string changeVector) => Merge(this, new ChangeVector(changeVector));

    public void UpdateOrder(string nodeTag, string dbId, long etag) => UpdateInternal(nodeTag, dbId, etag, Order);

    public void UpdateVersion(string nodeTag, string dbId, long etag) => UpdateInternal(nodeTag, dbId, etag, Version);

    public static ConflictStatus GetConflictStatusForDocument(ChangeVector documentVector1, ChangeVector documentVector2) => GetConflictStatusInternal(documentVector1.Version, documentVector2.Version);

    public static ConflictStatus GetConflictStatusForDatabase(ChangeVector documentVector, ChangeVector databaseVector) => GetConflictStatusInternal(documentVector.Order, databaseVector.Order);

    private void UpdateInternal(string nodeTag, string dbId, long etag, ChangeVector changeVector)
    {
        EnsureValid();
        var result = ChangeVectorUtils.TryUpdateChangeVector(nodeTag, dbId, etag, changeVector);
        if (result.IsValid)
        {
            changeVector._changeVector = result.ChangeVector;
        }
    }

    private static ConflictStatus GetConflictStatusInternal(ChangeVector remote, ChangeVector local)
    {
        remote.EnsureValid();
        local.EnsureValid();

        return ChangeVectorUtils.GetConflictStatus(remote.AsString(), local.AsString());
    }

    public static ChangeVector Merge(ChangeVector cv1, ChangeVector cv2)
    {
        if (cv1 == null)
            return cv2;
            
        if (cv2 == null)
            return cv1;

        if (cv1.IsSingle && cv2.IsSingle)
        {
            var result = ChangeVectorUtils.MergeVectors(cv1._changeVector, cv2._changeVector);
            return new ChangeVector(result);
        }

        var orderMerge = ChangeVectorUtils.MergeVectors(cv1.Order._changeVector, cv2.Order._changeVector);
        var versionMerge = ChangeVectorUtils.MergeVectors(cv1.Version._changeVector, cv2.Version._changeVector);
        return new ChangeVector(versionMerge, orderMerge);
    }

    public static ChangeVector Merge(List<ChangeVector> changeVectors)
    {
        var result = changeVectors[0];
        for (int i = 1; i < changeVectors.Count; i++)
        {
            result = Merge(result, changeVectors[i]);
        }

        return result;
    }

    public bool RemoveIds(HashSet<string> ids)
    {
        if (IsNullOrEmpty)
            return false;

        if (ids == null)
            return false;

        if (string.IsNullOrEmpty(_changeVector) == false)
        {
            var entries = _changeVector.ToChangeVectorList();
            if (entries.RemoveAll(x => ids.Contains(x.DbId)) > 0)
            {
                _changeVector = entries.SerializeVector();
                return true;
            }

            return false;
        }
            
        return Version.RemoveIds(ids) | Order.RemoveIds(ids);
    }

    public void StripTags(string tag, string exclude)
    {
        if (IsNullOrEmpty)
            return;

        if (string.IsNullOrEmpty(_changeVector) == false)
        {
            _changeVector = StripTags(_changeVector, tag, exclude);
            return;
        }
            
        Order.StripTags(tag, exclude);
        Version.StripTags(tag, exclude);
    }

    public void StripTrxnTags() => StripTags(ChangeVectorParser.TrxnTag, exclude: null);
        
    public void StripSinkTags(string exclude) => StripTags(ChangeVectorParser.SinkTag, exclude);

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
