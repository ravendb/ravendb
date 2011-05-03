namespace Raven.Studio
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Windows;
    using Caliburn.Micro;

    public static class StudioViewLocator
    {
        public static UIElement LocateForModelType(
            Type modelType,
            DependencyObject viewLocation,
            object context,
            Func<Type, DependencyObject, object, UIElement> original)
        {
            // case 1: types that are not in the Studio assembly
            if (modelType.Assembly != Assembly.GetExecutingAssembly())
            {
                UIElement view;
                if (TryResolveViewFromExternalAssembly(modelType, context, out view))
                {
                    return view;
                }
            }

            // case 2: simplified convention
            string viewTypeName = modelType.FullName + "View";
            Type viewType = (from assmebly in AssemblySource.Instance
                             from type in assmebly.GetExportedTypes()
                             where type.FullName == viewTypeName
                             select type).FirstOrDefault();

            if (viewType != null)
                return ViewLocator.GetOrCreateViewType(viewType);

            // case 3: apply the default when all else fails
            return original(modelType, viewLocation, context);
        }

        static bool TryResolveViewFromExternalAssembly(Type modelType, object context, out UIElement view)
        {
            view = null;

            if (TryResolveViewFromPluginAssembly(modelType, context, out view))
            {
                return true;
            }

            return false;
        }

        static bool TryResolveViewFromApi(Type modelType, object context, out UIElement view)
        {
            view = null;
            var name = "Raven.Studio.Data." + modelType.Name;

            if (name.Contains("`"))
                name = name.Substring(0, name.IndexOf("`"));

            var viewType = GetViewType(modelType, context, AssemblySource.Instance.ToArray());

            if (viewType != null)
            {
                view = ViewLocator.GetOrCreateViewType(viewType);
                return true;
            }

            return false;
        }

        static bool TryResolveViewFromPluginAssembly(Type modelType, object context, out UIElement view)
        {
            view = null;
            var viewType = GetViewType(modelType, context, modelType.Assembly);
            if (viewType != null)
            {
                view = ViewLocator.GetOrCreateViewType(viewType);
                return true;
            }
            return false;
        }

        static Type GetViewType(Type modelType, object context, params Assembly[] sources)
        {
            foreach (var viewTypeName in PossibleViewTypeNames(modelType, context))
            {
                var viewType = (from assembly in sources
                                from type in assembly.GetExportedTypes()
                                where type.FullName == viewTypeName
                                select type).FirstOrDefault();
                if (viewType != null) return viewType;
            }

            return null;
        }

        static IEnumerable<string> PossibleViewTypeNames(Type modelType, object context)
        {
            return PossibleViewTypeNamesInternal(modelType.FullName, context)
                    .Where(x => x != modelType.FullName);
        }

        static IEnumerable<string> PossibleViewTypeNamesInternal(string fullName, object context)
        {
            if (context != null)
            {
                yield return fullName.Remove(fullName.Length - 4, 4);
                yield return fullName + "." + context;
            }

            yield return fullName + "View";
            yield return fullName.Replace("Model", "View");
            yield return fullName.Replace("ViewModel", "View");
        }
    }
}