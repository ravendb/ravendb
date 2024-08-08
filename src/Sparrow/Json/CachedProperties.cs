using System;
using System.Linq;
using System.Collections.Generic;
using Sparrow.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Binary;

namespace Sparrow.Json
{
    /// <summary>
    /// This class allows us to cache the properties from multiple documents
    /// That way, we don't need to recompute the sort order.
    /// 
    /// However, because different documents may have different fields, we're tracking
    /// which properties we have seen and which are new. If the order in which we read the 
    /// properties doesn't match the cached version, we'll clear the cache up to the point the
    /// properties match. 
    /// 
    /// This is done, so we'll not write properties that don't belong to that document, but to 
    /// previous documents handled in the same batch
    /// </summary>
    public sealed class CachedProperties : IDisposable
    {
        private readonly JsonOperationContext _context;

        private Sorter<AbstractBlittableJsonDocumentBuilder.PropertyTag, PropertySorter> _sorter;


        private static readonly PerCoreContainer<CachedSort[]> _perCorePropertiesCache = new();

        public void Reset()
        {
            _docPropNames.WeakClear();
            _propertiesSortOrder.Clear();
            _propertyNameToId.Clear();
            _propertiesNeedSorting = false;
            _hasDuplicates = false;
            
            PropertiesDiscovered = 0;
            DocumentNumber = 0;
        }

        private readonly struct PropertySorter(CachedProperties props) : IComparer<AbstractBlittableJsonDocumentBuilder.PropertyTag>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(
                AbstractBlittableJsonDocumentBuilder.PropertyTag x, 
                AbstractBlittableJsonDocumentBuilder.PropertyTag y)
            {
                var compare = x.Property.GlobalSortOrder - y.Property.GlobalSortOrder;
                if (compare != 0) 
                    return compare;
                
                props._hasDuplicates = true;
                return y.Position - x.Position;
            }
        }

        public sealed class PropertyName(int hash, LazyStringValue comparer, int globalSortOrder, int propertyId) : IComparable<PropertyName>
        {
            public readonly int HashCode = hash;

            public readonly LazyStringValue Comparer = comparer;
            public int GlobalSortOrder = globalSortOrder;
            public int PropertyId = propertyId;

            public int CompareTo(PropertyName other)
            {
                return Comparer.CompareTo(other.Comparer);
            }

            public override string ToString()
            {
                return $"Value: {Comparer}, GlobalSortOrder: {GlobalSortOrder}, PropertyId: {PropertyId}";
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(PropertyName other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return HashCode == other.HashCode;
            }                    

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                return obj is PropertyName a && HashCode == a.HashCode;
            }

            public override int GetHashCode()
            {
                return HashCode;
            }
        }

        private sealed class PropertyPosition(PropertyName property, int sortedPosition)
        {
            public readonly PropertyName Property = property;
            public int SortedPosition = sortedPosition;
            public AbstractBlittableJsonDocumentBuilder.PropertyTag Tmp;
        }

        private sealed class CachedSort
        {
            public readonly FastList<PropertyPosition> Sorting = new();
            public int FinalCount;

            public override string ToString()
            {
                return string.Join(", ", Sorting.Select(x => x.Property.Comparer));
            }

            public void Clear()
            {
                FinalCount = 0;
                Sorting.Clear();
            }
        }

        private readonly CachedSort[] _cachedSorts;
        private const int CachedSortsSize = 512;
        public static int CachedPropertiesSize = 512;

        private readonly FastList<PropertyName> _docPropNames = new();
        private readonly SortedDictionary<PropertyName, object> _propertiesSortOrder = new();
        private readonly Dictionary<LazyStringValue, PropertyName> _propertyNameToId = new(default(LazyStringValueStructComparer));
        
        private bool _propertiesNeedSorting;
        public int PropertiesDiscovered;

        public CachedProperties(JsonOperationContext context)
        {
            _context = context;
            _sorter = new Sorter<AbstractBlittableJsonDocumentBuilder.PropertyTag, PropertySorter>(new PropertySorter(this));

            if (_perCorePropertiesCache.TryPull(out _cachedSorts) == false)
                _cachedSorts = new CachedSort[CachedSortsSize]; // size is fixed and used in GetPropertiesHashedIndex
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PropertyName GetProperty(LazyStringValue propName)
        {
            if (_propertyNameToId.TryGetValue(propName, out PropertyName prop))
            {
                // PERF: This is the most common scenario, we need it to come first. 
                if (prop.PropertyId < PropertiesDiscovered)
                    return prop;

                if (prop.PropertyId != PropertiesDiscovered)
                    prop = SwapPropertyIds(prop);
                
                PropertiesDiscovered++;
                return prop;
            }

            return UnlikelyGetProperty(propName);
        }

        private PropertyName UnlikelyGetProperty(LazyStringValue propName)
        {
            var propIndex = _docPropNames.Count;
            propName = _context.GetLazyStringForFieldWithCaching(propName);

            // PERF: The hash for the property needs to be a hash code, if its not
            //       we will be paying the cost of hash collisions in the sort checks.
            var prop = new PropertyName(propName.GetHashCode(), propName, -1, propIndex);

            _docPropNames.Add(prop);
            _propertiesSortOrder.Add(prop, prop);
            _propertyNameToId[propName] = prop;
            _propertiesNeedSorting = true;
            
            if (_docPropNames.Count > PropertiesDiscovered + 1)
                prop = SwapPropertyIds(prop);

            PropertiesDiscovered++;
            return prop;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PropertyName SwapPropertyIds(PropertyName prop)
        {
            // this property doesn't match the order that we previously saw the properties.
            // it is possible that this is a completely new format, or just properties
            // in different order. 
            // we'll assume the later and move the property around, this is safe to 
            // do because we ignore the properties showing up after the PropertiesDiscovered

            int xPropertyId = PropertiesDiscovered;
            int yPropertyId = prop.PropertyId;

            var x = _docPropNames[xPropertyId];           
            var y = _docPropNames[yPropertyId];

            x.PropertyId = yPropertyId;
            y.PropertyId = xPropertyId;

            _docPropNames[xPropertyId] = y;
            _docPropNames[yPropertyId] = x;

            return y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort(FastList<AbstractBlittableJsonDocumentBuilder.PropertyTag> properties)
        {
            var index = GetPropertiesHashedIndex(properties);

            // Sort object properties metadata by property names
            if (_propertiesNeedSorting)
            {
                UpdatePropertiesSortOrder();
            }

            var cachedSort = _cachedSorts[index];
            if (cachedSort?.Sorting.Count != properties.Count)
            {
                UnlikelySortProperties(properties);
                return;
            }

            // we are frequently going to see documents with ids in the same order
            // so we can take advantage of that by remember the previous sort, we 
            // check if the values are the same, and if so, save the sort

            var sortingList = cachedSort.Sorting;
            for (int i = 0; i < properties.Count; i++)
            {
                var sortingProp = sortingList[i];
                var sortedProp = properties[i];

                if (sortingProp.Property.Equals(sortedProp.Property))
                {
                    sortingProp.Tmp = sortedProp;
                }
                else
                {
                    UnlikelySortProperties(properties);
                    return;
                }
            }

            // ReSharper disable once ForCanBeConvertedToForeach
            int sortingListCount = sortingList.Count;
            for (int i = 0; i < sortingListCount; i++)
            {
                var sortingProp = sortingList[i];
                properties[sortingProp.SortedPosition] = sortingProp.Tmp;
            }

            int finalCount = cachedSort.FinalCount;
            if (properties.Count != finalCount)
            {
                properties.RemoveRange(finalCount, properties.Count - finalCount);
            }            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetPropertiesHashedIndex(FastList<AbstractBlittableJsonDocumentBuilder.PropertyTag> properties)
        {
            int count = properties.Count;

            int hash = 0;
            for (int i = 0; i < count; i++)
            {
                // Because we are using a HashCombiner to avoid hash collisions, the HashCode should be
                // an actual hash. Failing to do so will cause hash collisions. 
                hash = Hashing.HashCombiner.CombineInline(hash, properties[i].Property.HashCode);
            }

            Debug.Assert(_cachedSorts.Length == CachedSortsSize && Bits.PowerOf2(CachedSortsSize) == CachedSortsSize); 

            hash &= (CachedSortsSize-1); // % CachedSortsSize 
            return hash;
        }

        private void UnlikelySortProperties(FastList<AbstractBlittableJsonDocumentBuilder.PropertyTag> properties)
        {
            _hasDuplicates = false;

            var index = GetPropertiesHashedIndex(properties);

            _cachedSorts[index] ??= new CachedSort();

            var cachedSort = _cachedSorts[index];
            var sorting = cachedSort.Sorting;
            
            sorting.Clear();
            for (int i = 0; i < properties.Count; i++)
            {
                sorting.Add(new PropertyPosition(properties[i].Property, -1));
            }

            cachedSort.FinalCount = properties.Count;

            properties.Sort(ref _sorter);

            // The item comparison method has a side effect, which can modify the _hasDuplicates field.
            // This can either be true or false at any given time. 
            if (_hasDuplicates)
            {
                // leave just the latest
                for (int i = 0; i < properties.Count - 1; i++)
                {
                    if (properties[i].Property.Equals(properties[i + 1].Property))
                    {
                        cachedSort.FinalCount--;
                        sorting[i + 1] = new PropertyPosition(
                            properties[i + 1].Property,
                            // set it to the previous value, so it'll just overwrite
                            // this saves us a check and more complex code
                            sortedPosition: i
                        );

                        properties.RemoveAt(i + 1);

                        i--;
                    }
                }
            }            

            foreach (var propPos in sorting)
            {
                propPos.SortedPosition = -1;
                for (int j = 0; j < properties.Count; j++)
                {
                    if (properties[j].Property != propPos.Property) 
                        continue;
                    
                    propPos.SortedPosition = j;
                    break;
                }
            }

        }

        private void UpdatePropertiesSortOrder()
        {
            int index = 0;
            foreach (var o in _propertiesSortOrder)
            {
                o.Key.GlobalSortOrder = index++;
            }
            _propertiesNeedSorting = false;
        }

        private bool _hasDuplicates;
        internal int DocumentNumber { get; private set; }

        public LazyStringValue GetProperty(int index)
        {
            return _docPropNames[index].Comparer;
        }

        public void NewDocument()
        {
            DocumentNumber++;
            PropertiesDiscovered = 0;
        }

        public bool NeedClearPropertiesCache()
        {
            return PropertiesDiscovered > CachedPropertiesSize;
        }

        public void Dispose()
        {
            // We don't have anything to do. 
            if (_cachedSorts == null)
                return;

            foreach (CachedSort sort in _cachedSorts)
            {
                sort?.Clear();
            }
            _perCorePropertiesCache.TryPush(_cachedSorts);
        }
    }
}
