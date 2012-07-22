using System.ComponentModel;

namespace Raven.Studio.Infrastructure
{
    public class VirtualItem<T> : INotifyPropertyChanged where T : class
    {
        private readonly VirtualCollection<T> _parent;
        private readonly int _index;
        private T _item;
        private bool _isStale;
        private bool dataFetchError;

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
                if (!IsRealized && !DataFetchError)
                {
                    _parent.RealizeItemRequested(Index);
                }
                return _item;
            }
            private set
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

        public void SupplyValue(T value)
        {
            DataFetchError = false;
            Item = value;
        }

        public void ClearValue()
        {
            DataFetchError = false;
            Item = null;
        }

        public void ErrorFetchingValue()
        {
            Item = null;
            DataFetchError = true;
        }

        public bool DataFetchError
        {
            get { return dataFetchError; }
            private set
            {
                dataFetchError = value;
                OnPropertyChanged(new PropertyChangedEventArgs("DataFetchError"));
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
