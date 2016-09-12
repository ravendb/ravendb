using System.Collections.Generic;

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
    /// This is done so we'll not write properties that don't belong to that document, but to 
    /// previous documents handled in the same batch
    /// </summary>
    public class CachedProperties : IComparer<BlittableJsonDocumentBuilder.PropertyTag>
    {
        private readonly JsonOperationContext _context;

        private class PropertyName
        {
            public LazyStringValue Comparer;
            public int GlobalSortOrder;
            public int PropertyId;

            public override string ToString()
            {
                return $"Value: {Comparer}, GlobalSortOrder: {GlobalSortOrder}, PropertyId: {PropertyId}";
            }
        }

        private readonly List<PropertyName> _docPropNames = new List<PropertyName>();
        private readonly List<PropertyName> _propertiesSortOrder = new List<PropertyName>();
        private readonly Dictionary<LazyStringValue, PropertyName> _propertyNameToId = new Dictionary<LazyStringValue, PropertyName>();
        private bool _propertiesNeedSorting;

        public int PropertiesDiscovered;

        public CachedProperties(JsonOperationContext context)
        {
            _context = context;
        }

        public int GetPropertyId(LazyStringValue propName)
        {
            PropertyName prop;
            if (_propertyNameToId.TryGetValue(propName, out prop) == false)
            {
                var propIndex = _docPropNames.Count;
                propName = _context.GetLazyStringForFieldWithCaching(propName);
                prop = new PropertyName
                {
                    Comparer = propName,
                    GlobalSortOrder = -1,
                    PropertyId = propIndex
                };

                _docPropNames.Add(prop);
                _propertiesSortOrder.Add(prop);
                _propertyNameToId[propName] = prop; 
                _propertiesNeedSorting = true;
                if (_docPropNames.Count > PropertiesDiscovered+1)
                {
                    prop = SwapPropertyIds(prop);
                }
                PropertiesDiscovered++;
            }
            else if (prop.PropertyId >= PropertiesDiscovered)
            {
                if (prop.PropertyId != PropertiesDiscovered)
                {
                    prop = SwapPropertyIds(prop);
                }
                PropertiesDiscovered++;
            }
            return prop.PropertyId;
        }

        private PropertyName SwapPropertyIds(PropertyName prop)
        {
            // this property doesn't match the order that we previously saw the properties.
            // it is possible that this is a completely new format, or just properties
            // in different order. 
            // we'll assume the later and move the property around, this is safe to 
            // do because we ignore the properties showing up after the PropertiesDiscovered

            var old = _docPropNames[PropertiesDiscovered];
            _docPropNames[PropertiesDiscovered] = _docPropNames[prop.PropertyId];
            old.PropertyId = _docPropNames[PropertiesDiscovered].PropertyId;
            _docPropNames[old.PropertyId] = old;
            prop = _docPropNames[PropertiesDiscovered];
            prop.PropertyId = PropertiesDiscovered;
            return prop;
        }

        public void Sort(List<BlittableJsonDocumentBuilder.PropertyTag> properties)
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
            _hasDuplicates = false;
            properties.Sort(this);
            if (_hasDuplicates)
            {
                // leave just the latest
                for (int i = 0; i < properties.Count-1; i++)
                {
                    if (properties[i].PropertyId == properties[i + 1].PropertyId)
                    {
                        properties.RemoveAt(i + 1);
                        i--;
                    }
                }
            }

        }

        private bool _hasDuplicates;

        int IComparer<BlittableJsonDocumentBuilder.PropertyTag>.Compare(BlittableJsonDocumentBuilder.PropertyTag x, BlittableJsonDocumentBuilder.PropertyTag y)
        {
            var compare = _docPropNames[x.PropertyId].GlobalSortOrder - _docPropNames[y.PropertyId].GlobalSortOrder;
            if (compare == 0)
            {
                _hasDuplicates = true;
                return y.Position - x.Position;
            }
            return compare;
        }

        public LazyStringValue GetProperty(int index)
        {
            return _docPropNames[index].Comparer;
        }

        public void NewDocument()
        {
            PropertiesDiscovered = 0;
        }
    }
}