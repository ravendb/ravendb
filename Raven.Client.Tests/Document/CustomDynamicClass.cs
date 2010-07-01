using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.Diagnostics;

namespace Raven.Client.Tests.Document
{    
    //from http://msdn.microsoft.com/en-us/library/system.dynamic.dynamicobject.aspx
    class CustomDynamicClass : DynamicObject
    {
        // The inner dictionary, used to store the new dynamic members and their values
		public Dictionary<string, object> Properties { get; set; }

    	public CustomDynamicClass()
    	{
			Properties = new Dictionary<string, object>();
    	}


        // If you try to get a value of a property not defined in the class, this method is called.
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            // Converting the property name to lowercase so that property names become case-insensitive.
            string name = binder.Name.ToLower();            

            // If the property name is found in a dictionary, set the result parameter to the 
            // property value and return true. Otherwise, return false.
            return Properties.TryGetValue(name, out result);
        }

        // If you try to set a value of a property that is not defined in the class, this method is called.
        public override bool TrySetMember(SetMemberBinder binder, object value)
        {            
            // Converting the property name to lowercase so that property names become case-insensitive.
            Properties[binder.Name.ToLower()] = value;            

            // You can always add a value to a dictionary,
            // so this method always returns true.
            return true;
        }

        public override IEnumerable<string> GetDynamicMemberNames()
        {
            return Properties.Keys;
        }
    }    
}
