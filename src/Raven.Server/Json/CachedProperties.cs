using System;
using System.Collections.Generic;

namespace Raven.Server.Json
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
    /// This is done so we'll not write properties that don't belong to that document, but to 
    /// previous documents handled in the same batch
    /// </summary>
    public class CachedProperties : IComparer<BlittableJsonWriter.PropertyTag>
    {
        private readonly RavenOperationContext _context;

        private class PropertyName
        {
            public LazyStringValue Comparer;
            public string Value;
            public int GlobalSortOrder;
        }

        private readonly List<PropertyName> _docPropNames = new List<PropertyName>();
        private readonly List<PropertyName> _propertiesSortOrder = new List<PropertyName>();
        private readonly Dictionary<string, int> _propertyNameToId = new Dictionary<string, int>(StringComparer.Ordinal);
        private bool _propertiesNeedSorting;

        public int PropertiesDiscovered;

        public CachedProperties(RavenOperationContext context)
        {
            _context = context;
        }

        public int GetPropertyId(string propName)
        {
            int propIndex;
            if (_propertyNameToId.TryGetValue(propName, out propIndex) == false)
            {
                PropertiesDiscovered++;
                propIndex = _propertyNameToId.Count;
                _propertyNameToId[propName] = propIndex;
                var propertyName = new PropertyName
                {
                    Comparer = _context.GetComparerFor(propName),
                    Value = propName,
                    GlobalSortOrder = -1
                };
                _docPropNames.Add(propertyName);
                _propertiesSortOrder.Add(propertyName);
                _propertiesNeedSorting = true;
            }
            if (propIndex >= PropertiesDiscovered)
            {
                if (propIndex != PropertiesDiscovered)
                {
                    // this property doesn't match the order that we previously saw the properties.
                    // it is possible that this is a completely new format, or just properties
                    // in different order. 
                    // we'll assume the later and move the property around, this is safe to 
                    // do because we ignore the properties showing up after the PropertiesDiscovered

                    var old = _docPropNames[PropertiesDiscovered];
                    _docPropNames[PropertiesDiscovered] = _docPropNames[propIndex];
                    _propertyNameToId[old.Value] = propIndex;
                    _propertyNameToId[propName] = PropertiesDiscovered;
                    propIndex = PropertiesDiscovered;
                }
                PropertiesDiscovered++;
            }
            return propIndex;
        }

        public void Sort(List<BlittableJsonWriter.PropertyTag> properties)
        {
            // Sort object properties metadata by property names
            if (_propertiesNeedSorting)
            {
                _propertiesSortOrder.Sort((a1, a2) => a1.Comparer.CompareTo(a2.Comparer));
                for (int i = 0; i < _propertiesSortOrder.Count; i++)
                {
                    _propertiesSortOrder[i].GlobalSortOrder = i;
                }
                _propertiesNeedSorting = false;
            }
            properties.Sort(this);

        }


        int IComparer<BlittableJsonWriter.PropertyTag>.Compare(BlittableJsonWriter.PropertyTag x, BlittableJsonWriter.PropertyTag y)
        {
            return _docPropNames[x.PropertyId].GlobalSortOrder - _docPropNames[y.PropertyId].GlobalSortOrder;
        }

        public string GetProperty(int index)
        {
            return _docPropNames[index].Value;
        }

        public void NewDocument()
        {
            PropertiesDiscovered = 0;
        }
    }
}