using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.Linq.Expressions;
using System.Diagnostics;

namespace Raven.Client.Tests.Document
{
	public class CustomDynamicClass : DynamicObject
    {
    	readonly Dictionary<string, object> dictionary = new Dictionary<string, object>();

        public int Count
        {
            get { return dictionary.Count; }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            string name = binder.Name;            

            return dictionary.TryGetValue(name, out result);
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {            
            dictionary[binder.Name] = value;            
            return true;
        }

        public override IEnumerable<string> GetDynamicMemberNames()
        {
            return dictionary.Keys;
        }
    }    
}
