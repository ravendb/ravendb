namespace Raven.Web.Utils
{
    using System;
    using System.Reflection;

    internal static class UnsafeIISMethods
    {
        private static readonly Lazy<UnsafeIISMethodsWrapper> IIS = new Lazy<UnsafeIISMethodsWrapper>(() => new UnsafeIISMethodsWrapper());

        public static bool RequestedAppDomainRestart
        {
            get
            {
                if (IIS.Value.CheckConfigChanged == null)
                {
                    return false;
                }

                return !IIS.Value.CheckConfigChanged();
            }
        }

        public static bool CanDetectAppDomainRestart
        {
            get { return IIS.Value.CheckConfigChanged != null; }
        }

        private class UnsafeIISMethodsWrapper
        {
            public UnsafeIISMethodsWrapper()
            {
                // Private reflection to get the UnsafeIISMethods
                Type type = Type.GetType("System.Web.Hosting.UnsafeIISMethods, System.Web, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a");

                if (type == null)
                {
                    return;
                }

                // This method can tell us if ASP.NET requested and app domain shutdown
                MethodInfo methodInfo = type.GetMethod("MgdHasConfigChanged", BindingFlags.NonPublic | BindingFlags.Static);

                if (methodInfo == null)
                {
                    // Method signature changed so just bail
                    return;
                }

                try
                {
                    CheckConfigChanged = (Func<bool>)Delegate.CreateDelegate(typeof(Func<bool>), methodInfo);
                }
                catch (ArgumentException)
                {
                }
                catch (MissingMethodException)
                {
                }
                catch (MethodAccessException)
                {
                }
                // If we failed to create the delegate we can't do the check reliably
            }

            public Func<bool> CheckConfigChanged { get; private set; }
        }
    }
}