// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.

namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using System.Collections.Generic;
	using System.Reflection;
	using System.Threading.Tasks;
	using Microsoft.Silverlight.Testing.Harness;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

    public class TestMethod : ITestMethod
    {
		public static Type ReturnTypeForAsyncTaskTest { get; private set; }

		static TestMethod()
		{
			ReturnTypeForAsyncTaskTest = typeof(IEnumerable<Task>);
		}

        private const string ContextPropertyName = "TestContext";
        private const int DefaultPriority = 3;
        private static readonly object[] None = { };
        private readonly MethodInfo methodInfo;

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
                var description = this.GetAttribute<DescriptionAttribute>();
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
                var owner = this.GetAttribute<OwnerAttribute>();
                return owner == null ? null : owner.Owner;
            }
        }


        public IExpectedException ExpectedException
        {
            get
            {
                var exp = this.GetAttribute<ExpectedExceptionAttribute>();
                return exp != null ? new ExpectedException(exp) : null;
            }
        }

        public int? Timeout
        {
            get
            {
                var timeout = this.GetAttribute<TimeoutAttribute>();
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
                var pri = this.GetAttribute<PriorityAttribute>();
                return new Priority(pri == null ? DefaultPriority : pri.Priority);
            }
        }

        public virtual IEnumerable<Attribute> GetDynamicAttributes()
        {
            return new Attribute[] { };
        }


        public virtual void Invoke(object instance)
        {
            if (methodInfo.ReturnType == typeof(Task) && typeof(AsynchronousTaskTest).IsAssignableFrom(instance.GetType()))
            {
                var asynTask = (AsynchronousTaskTest) instance;
                asynTask.ExecuteTaskTest(methodInfo);
            }
            else
			if (ReturnTypeForAsyncTaskTest.IsAssignableFrom(methodInfo.ReturnType) && typeof(AsynchronousTaskTest).IsAssignableFrom(instance.GetType()))
			{
                var asynTask = (AsynchronousTaskTest)instance;
                asynTask.ExecuteTest(methodInfo);
			} else
			{
			    methodInfo.Invoke(instance, None);
			}
        }

        public override string ToString()
        {
            return Name;
        }
    }

    public static class TestMethodExtensions
    {
        public static T GetAttribute<T>(this TestMethod testMethod) where T : Attribute
        {
            return ReflectionUtility.GetAttribute(testMethod, typeof(T), true) as T;
        } 
    }
}