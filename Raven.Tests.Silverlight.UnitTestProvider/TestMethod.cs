namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using System.Collections.Generic;
	using System.Reflection;
	using Microsoft.Silverlight.Testing.Harness;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

    public class TestMethod : ITestMethod
    {
        private const string ContextPropertyName = "TestContext";
        private const int DefaultPriority = 3;
        private static readonly object[] None = { };
        private readonly MethodInfo methodInfo;

        /// <summary>
        /// Private constructor, the constructor requires the method reflection object.
        /// </summary>
        private TestMethod() { }

        public TestMethod(MethodInfo methodInfo) : this()
        {
            this.methodInfo = methodInfo;
        }

        public event EventHandler<StringEventArgs> WriteLine;

        internal void OnWriteLine(string s)
        {
            var handler = WriteLine;
            if (handler != null)
            {
                handler(this, new StringEventArgs(s));
            }
        }

        public void DecorateInstance(object instance)
        {
            if (instance == null)
            {
                return;
            }

            Type t = instance.GetType();
            PropertyInfo pi = t.GetProperty(ContextPropertyName, BindingFlags.Public | BindingFlags.Instance);
            if (pi != null && pi.CanWrite)
            {
                try
                {
                    var utc = new UnitTestContext(this);
                    pi.SetValue(instance, utc, null);
                }
                finally
                {
                }
            }
        }

        public MethodInfo Method 
        { 
            get { return methodInfo; }
        }

        public bool Ignore
        {
            get { return ReflectionUtility.HasAttribute(this, ProviderAttributes.IgnoreAttribute); }
        }

        public string Description
        {
            get
            {
                var description = ReflectionUtility.GetAttribute(
                    this,
                    ProviderAttributes.DescriptionAttribute,
                    true) as DescriptionAttribute;
                return description != null ? description.Description : null;
            }
        }

        public virtual string Name
        {
            get { return methodInfo.Name; }
        }

        public string Category
        {
            get { return null; }
        } 

        public string Owner
        {
            get
            {
                var owner = ReflectionUtility.GetAttribute(
                    this,
                    ProviderAttributes.OwnerAttribute) as
                    OwnerAttribute;
                return owner == null ? null : owner.Owner;
            }
        }


        public IExpectedException ExpectedException
        {
            get
            {
                var exp = ReflectionUtility.GetAttribute(
                    this,
                    ProviderAttributes.ExpectedExceptionAttribute) as
                    ExpectedExceptionAttribute;
                return exp != null ? new ExpectedException(exp) : null;
            }
        }

        public int? Timeout
        {
            get
            {
                var timeout = ReflectionUtility.GetAttribute(
                    this,
                    ProviderAttributes.TimeoutAttribute,
                    true) as TimeoutAttribute;
                return timeout != null ? (int?)timeout.Timeout : null;
            }
        }

        public ICollection<ITestProperty> Properties
        {
            get
            {
                var properties = new List<ITestProperty>();
                var attributes = ReflectionUtility.GetAttributes(
                    this,
                    ProviderAttributes.TestProperty,
                    true);
                if (attributes != null)
                {
                    foreach (Attribute a in attributes)
                    {
                        TestPropertyAttribute tpa = a as TestPropertyAttribute;
                        if (tpa != null)
                        {
                            properties.Add(new TestProperty(tpa.Name, tpa.Value));
                        }
                    }
                }

                return properties;
            }
        }

        public ICollection<IWorkItemMetadata> WorkItems
        {
            get { return null; }
        }

        public IPriority Priority
        {
            get 
            {
                var pri = ReflectionUtility.GetAttribute(this, ProviderAttributes.Priority, true) as PriorityAttribute;
                return new Priority(pri == null ? DefaultPriority : pri.Priority);
            }
        }

        public virtual IEnumerable<Attribute> GetDynamicAttributes()
        {
            return new Attribute[] { };
        }


        public virtual void Invoke(object instance)
        {
			//if((typeof(IEnumerable<>).IsAssignableFrom(_methodInfo.ReturnType))
            methodInfo.Invoke(instance, None);

        }

        public override string ToString()
        {
            return Name;
        }
    }
}