using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Infrastructure.Validators;
using Validation = Raven.Studio.Infrastructure.Validation;

namespace Raven.Studio.Features.Documents
{
    public class ColumnEditorViewModel : ViewModel, IEditableObject, INotifyDataErrorInfo, IValidationSuppressible
    {
        private IList<ValidationResult> validationResults = new List<ValidationResult>();
        public event EventHandler<EventArgs> ChangesCommitted;

        private ColumnDefinition column;
        private string header = string.Empty;
        private string binding = string.Empty;
        private string defaultWidth = string.Empty;

        public string Header
        {
            get { return header; }
            set
            {
                if (header == value)
                {
                    return;
                }

                header = value;
                OnPropertyChanged(() => Header);
                OnPropertyChanged(() => IsNewRow);
                Revalidate();
            }
        }

        [RequiredString]
        public string Binding
        {
            get { return binding; }
            set
            {
                if (binding == value)
                {
                    return;
                }

                binding = value;
                if (string.IsNullOrEmpty(Header))
                {
                    Header = SuggestColumnName(binding);
                }
                OnPropertyChanged(() => Binding);
                OnPropertyChanged(() => IsNewRow);
                Revalidate();
            }
        }

        private string SuggestColumnName(string binding)
        {
            if (binding.StartsWith("$Meta:"))
                return binding.Substring("$Meta:".Length);
            if (binding.StartsWith("$JsonDocument:"))
                return binding.Substring("$JsonDocument:".Length);
            
            return binding;
        }

        [DataGridLength]
        public string DefaultWidth
        {
            get { return defaultWidth; }
            set
            {
                if (defaultWidth == value)
                    return;

                defaultWidth = value;
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
                header = column.Header;
                binding = column.Binding;
                defaultWidth = column.DefaultWidth;
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
            Revalidate();
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
            return new ColumnDefinition
                       {
                           Header = header,
                           Binding = binding,
                           DefaultWidth = defaultWidth
                       };
        }

        public event EventHandler<DataErrorsChangedEventArgs> ErrorsChanged;

        protected void OnErrorsChanged(DataErrorsChangedEventArgs e)
        {
            var handler = ErrorsChanged;
            if (handler != null) handler(this, e);
        }

        protected void OnChangesCommitted(EventArgs e)
        {
            var handler = ChangesCommitted;
            if (handler != null) handler(this, e);
        }

        public bool SuppressValidation
        {
            get { return IsNewRow; }
        }
    }
}