using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using System.Windows.Data;
using System.Globalization;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.Collections;

namespace Kieners.Silverlight
{
    public class BindingList : RelativeSourceBase, IList<RelativeSourceBinding>, ICollection<RelativeSourceBinding>, IEnumerable<RelativeSourceBinding>, IList, ICollection, IEnumerable
    {

        private List<RelativeSourceBinding> _internalList;

        public BindingList()
        {
            _internalList = new List<RelativeSourceBinding>();
        }

        #region IList<RelativeSourceBinding> Members

        public int IndexOf(RelativeSourceBinding item)
        {
            return _internalList.IndexOf(item);
        }

        public void Insert(int index, RelativeSourceBinding item)
        {
            _internalList.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            _internalList.RemoveAt(index);
        }

        public RelativeSourceBinding this[int index]
        {
            get
            {
                return _internalList[index];
            }
            set
            {
                _internalList[index] = value; ;
            }
        }

        #endregion

        #region ICollection<RelativeSourceBinding> Members

        public void Add(RelativeSourceBinding item)
        {
            _internalList.IndexOf(item);
        }

        public void Clear()
        {
            _internalList.Clear();
        }

        public bool Contains(RelativeSourceBinding item)
        {
            return _internalList.Contains(item);
        }

        public void CopyTo(RelativeSourceBinding[] array, int arrayIndex)
        {
            _internalList.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return _internalList.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(RelativeSourceBinding item)
        {
            return _internalList.Remove(item);
        }

        #endregion

        #region IEnumerable<RelativeSourceBinding> Members

        public IEnumerator<RelativeSourceBinding> GetEnumerator()
        {
            return _internalList.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _internalList.GetEnumerator();
        }

        #endregion

        #region IList Members

        int IList.Add(object value)
        {
            _internalList.Add((RelativeSourceBinding)value);
            return (this.Count - 1);
        }

        void IList.Clear()
        {
            _internalList.Clear();
        }

        bool IList.Contains(object value)
        {
            return _internalList.Contains((RelativeSourceBinding)value);
        }

        int IList.IndexOf(object value)
        {
            return _internalList.IndexOf((RelativeSourceBinding)value);
        }

        void IList.Insert(int index, object value)
        {
            _internalList.Insert(index, (RelativeSourceBinding)value);
        }

        bool IList.IsFixedSize
        {
            get { return false; }
        }

        bool IList.IsReadOnly
        {
            get { return false; }
        }

        void IList.Remove(object value)
        {
            _internalList.Remove((RelativeSourceBinding)value);
        }

        void IList.RemoveAt(int index)
        {
            _internalList.RemoveAt(index);
        }

        object IList.this[int index]
        {
            get
            {
                return _internalList[index];
            }
            set
            {
                _internalList[index] = (RelativeSourceBinding)value;
            }
        }

        #endregion

        #region ICollection Members

        void ICollection.CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
        }

        int ICollection.Count
        {
            get { return _internalList.Count; }
        }

        bool ICollection.IsSynchronized
        {
            get { return false; }
        }

        object ICollection.SyncRoot
        {
            get { return _internalList; }
        }

        #endregion
    }


}
