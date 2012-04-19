using System.ComponentModel;

namespace Raven.Studio.Infrastructure
{
    public class VirtualItem<T> : INotifyPropertyChanged where T : class
    {
        private readonly VirtualCollection<T> _parent;
        private readonly int _index;
        private T _item;
        private bool _isStale;

        public event PropertyChangedEventHandler PropertyChanged;

        public VirtualItem(VirtualCollection<T> parent, int index)
        {
            _parent = parent;
            _index = index;
        }

        public T Item
        {
            get
            {
                if (!IsRealized)
                {
                    _parent.RealizeItemRequested(Index);
                }
                return _item;
            }
            set
            {
                _item = value;
                OnPropertyChanged(new PropertyChangedEventArgs("Item"));
                OnPropertyChanged(new PropertyChangedEventArgs("IsRealized"));
                IsStale = false;
            }
        }

        public bool IsStale
        {
            get { return _isStale; }
            set
            {
                _isStale = value;
                OnPropertyChanged(new PropertyChangedEventArgs("IsStale"));
            }
        }

        public bool IsRealized { get { return _item != null; } }

        public int Index
        {
            get { return _index; }
        }

        public VirtualCollection<T> Parent
        {
            get { return _parent; }
        }

        protected void OnPropertyChanged(PropertyChangedEventArgs e)
        {
            PropertyChangedEventHandler handler = PropertyChanged;
            if (handler != null) handler(this, e);
        }
    }
}
