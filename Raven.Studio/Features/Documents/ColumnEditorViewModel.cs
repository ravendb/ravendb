using System;
using System.Collections;
using System.ComponentModel;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public class ColumnEditorViewModel : ViewModel, IEditableObject, INotifyDataErrorInfo
    {
        public event EventHandler<EventArgs> ChangesCommitted;

        private readonly ColumnModel column;
        private string header;
        private string binding;
        private string defaultWidth;

        public string Header
        {
            get { return this.header; }
            set { this.header = value; }
        }
        
        public string Binding
        {
            get { return this.binding; }
            set { this.binding = value; }
        }

        public string DefaultWidth
        {
            get { return this.defaultWidth; }
            set { this.defaultWidth = value; }
        }

        public ColumnEditorViewModel(ColumnModel column)
        {
            this.column = column;
            LoadPropertiesFromColumn();
        }

        public bool IsNewRow
        {
            get { return string.IsNullOrEmpty(Header) && string.IsNullOrEmpty(Binding); }
        }

        private void LoadPropertiesFromColumn()
        {
            this.header = column.Header;
            this.binding = column.Binding;
            this.defaultWidth = column.DefaultWidth;

            OnEverythingChanged();
        }

        private void SavePropertiesToColumn()
        {
            column.Header = header;
            column.Binding = binding;
            column.DefaultWidth = defaultWidth;
        }

        public void BeginEdit()
        {
            
        }

        public void EndEdit()
        {
            OnChangesCommitted(EventArgs.Empty);
        }

        public void CancelEdit()
        {
            LoadPropertiesFromColumn();
        }

        public IEnumerable GetErrors(string propertyName)
        {
            yield break;
        }

        public bool HasErrors
        {
            get { return false; }
        }

        public ColumnModel Column
        {
            get { return column; }
        }

        public event EventHandler<DataErrorsChangedEventArgs> ErrorsChanged;

        protected void OnChangesCommitted(EventArgs e)
        {
            EventHandler<EventArgs> handler = ChangesCommitted;
            if (handler != null) handler(this, e);
        }

        public void ApplyChanges()
        {
            SavePropertiesToColumn();
        }
    }
}
