using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
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
using Raven.Studio.Infrastructure.Validators;
using Validation = Raven.Studio.Infrastructure.Validation;

namespace Raven.Studio.Features.Documents
{
    public class ColumnEditorViewModel : ViewModel, IEditableObject, INotifyDataErrorInfo
    {
        private IList<ValidationResult> validationResults = new List<ValidationResult>();
        public event EventHandler<EventArgs> ChangesCommitted;

        private ColumnDefinition column;
        private string header;
        private string binding;
        private string defaultWidth;

        public string Header
        {
            get { return this.header; }
            set
            {
                this.header = value;
                OnPropertyChanged(() => Header);
                OnPropertyChanged(() => IsNewRow);
            }
        }

        public string Binding
        {
            get { return this.binding; }
            set
            {
                this.binding = value;
                OnPropertyChanged(() => Binding);
                OnPropertyChanged(() => IsNewRow);
            }
        }

        [DataGridLength]
        public string DefaultWidth
        {
            get { return this.defaultWidth; }
            set
            {
                this.defaultWidth = value;
                OnPropertyChanged(() => DefaultWidth);
                Revalidate();
            }
        }

        private void Revalidate()
        {
            Validation.Validate(this, validationResults,
                                property => OnErrorsChanged(new DataErrorsChangedEventArgs(property)));
            OnPropertyChanged(() => HasErrors);
        }

        public ColumnEditorViewModel()
        {
            
        }

        public ColumnEditorViewModel(ColumnDefinition column)
        {
            this.column = column;
            LoadPropertiesFromColumn();
        }

        public bool IsNewRow
        {
            get
            {
                return string.IsNullOrEmpty(Header) && string.IsNullOrEmpty(Binding) &&
                       string.IsNullOrEmpty(DefaultWidth);
            }
        }

        private void LoadPropertiesFromColumn()
        {
            if (column != null)
            {
                this.header = column.Header;
                this.binding = column.Binding;
                this.defaultWidth = column.DefaultWidth;
            }

            OnEverythingChanged();
        }

        private void SavePropertiesToColumn()
        {
            if (column == null)
            {
                column = new ColumnDefinition();
            }

            column.Header = header;
            column.Binding = binding;
            column.DefaultWidth = defaultWidth;
        }

        public void BeginEdit()
        {
            SavePropertiesToColumn();
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
            return validationResults.Where(v => v.MemberNames.Contains(propertyName)).Select(v => v.ErrorMessage);
        }

        public bool HasErrors
        {
            get { return validationResults.Count > 0; }
        }

        public ColumnDefinition GetColumn()
        {
            return new ColumnDefinition()
                       {
                           Header = header,
                           Binding = binding,
                           DefaultWidth = defaultWidth
                       };
        }

        public event EventHandler<DataErrorsChangedEventArgs> ErrorsChanged;

        protected void OnErrorsChanged(DataErrorsChangedEventArgs e)
        {
            EventHandler<DataErrorsChangedEventArgs> handler = ErrorsChanged;
            if (handler != null) handler(this, e);
        }

        protected void OnChangesCommitted(EventArgs e)
        {
            EventHandler<EventArgs> handler = ChangesCommitted;
            if (handler != null) handler(this, e);
        }
    }
}
