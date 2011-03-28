namespace Raven.Studio.Features.Statistics
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Dynamic;
    using System.Linq;
    using System.Reflection;

    //NOTE: this might be overkill, but it's done and it didn't take long ...
    public class DynamicViewModel<T> : DynamicObject, INotifyPropertyChanged where T:class 
    {
        public DynamicViewModel(T proxiedObject)
        {
            ProxiedObject = proxiedObject;
        }

        readonly Dictionary<string, object> extendedProperties = new Dictionary<string, object>();

        public T ProxiedObject { get; private set; }

        public event PropertyChangedEventHandler PropertyChanged = delegate { };

        public override IEnumerable<string> GetDynamicMemberNames()
        {
            return ProxiedObject.GetType()
                    .GetProperties()
                    .Select(x => x.Name)
                    .Union(extendedProperties.Keys);
        }

        public object this[string index]
        {
            get { return GetMember(index); }
            set { SetMember(index, value); RaisePropertyChanged(index); }
        }

        PropertyInfo GetPropertyInfo(string propertyName)
        {
            return ProxiedObject.GetType()
                    .GetProperties()
                    .FirstOrDefault(propertyInfo => propertyInfo.Name == propertyName);
        }

       void RaisePropertyChanged(string propertyName)
        {
            PropertyChanged(ProxiedObject, new PropertyChangedEventArgs(propertyName));
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = GetMember(binder.Name);
            return true;
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            SetMember(binder.Name, value);
            return true;
        }

        object GetMember(string propertyName)
        {
            var property = GetPropertyInfo(propertyName);
            if (property != null)
            {
                return property.GetValue(ProxiedObject, null);
            }
            
            if(extendedProperties.ContainsKey(propertyName))
            {
                return extendedProperties[propertyName];
            }
            
            throw new MissingMemberException("The property "+ propertyName + " does not exist.");
        }

        void SetMember(string propertyName, object value)
        {
            var property = GetPropertyInfo(propertyName);
            if(property != null)
            {
                property.SetValue(ProxiedObject, value, null);
            }
            else
            {
                extendedProperties[propertyName] = value;
            }
            
            RaisePropertyChanged(propertyName);
        }
    }
}