using System;
using System.Reflection;

namespace Rhino.DivanDB.Client
{
    public class DocumentConvention
    {
        public DocumentConvention()
        {
            FindIdentityProperty = q => q.Name == "Id";
        }

        public Func<PropertyInfo, bool> FindIdentityProperty { get; set; }
    }
}