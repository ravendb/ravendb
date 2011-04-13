namespace Raven.Studio.Framework
{
	using System;
	using System.Collections;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.ComponentModel.Composition.Hosting;
	using System.ComponentModel.Composition.Primitives;
	using System.ComponentModel.Composition.ReflectionModel;
	using System.Linq;
	using System.Reflection;

	public class ConventionalCatalog : ComposablePartCatalog
	{
		readonly List<ComposablePartDefinition> parts = new List<ComposablePartDefinition>();

		public override IQueryable<ComposablePartDefinition> Parts
		{
			get { return parts.AsQueryable(); }
		}

		public void RegisterType<TImplementation, TContract>()
		{
			RegisterType(typeof(TImplementation), typeof(TContract));
		}

		public void RegisterType(Type implementation, string key)
		{
			RegisterType(implementation, implementation, key);
		}

		public void RegisterWithTypeNameAsKey(Type implementation)
		{
			RegisterType(implementation, implementation, implementation.Name);
		}

		public void RegisterType(Type implementation, Type contract, string key = null)
		{
			var part = ReflectionModelServices.CreatePartDefinition(
				new Lazy<Type>(() => implementation),
				false,
				new Lazy<IEnumerable<ImportDefinition>>(() => GetImportDefinitions(implementation)),
				new Lazy<IEnumerable<ExportDefinition>>(() => GetExportDefinitions(implementation, contract, key)),
				new Lazy<IDictionary<string, object>>(() => new Dictionary<string, object>()),
				null
				);

			parts.Add(part);
		}

		static IEnumerable<ImportDefinition> GetImportDefinitions(Type implementationType)
		{
			var constructors = implementationType.GetConstructors()[0];
			var imports = new List<ImportDefinition>();

			foreach (var param in constructors.GetParameters())
			{
				var parameter = param;
				var cardinality = GetCardinality(parameter);
				var importType = cardinality == ImportCardinality.ZeroOrMore
				                 	? GetCollectionContractType(parameter.ParameterType)
				                 	: param.ParameterType;

				imports.Add(
					ReflectionModelServices.CreateImportDefinition(
						new Lazy<ParameterInfo>(() => parameter),
						AttributedModelServices.GetContractName(importType),
						AttributedModelServices.GetTypeIdentity(importType),
						Enumerable.Empty<KeyValuePair<string, Type>>(),
						cardinality,
						CreationPolicy.Any,
						null)
					);
			}

			return imports;
		}

		static ImportCardinality GetCardinality(ParameterInfo param)
		{
			return typeof(IEnumerable).IsAssignableFrom(param.ParameterType)
			       	? ImportCardinality.ZeroOrMore
			       	: ImportCardinality.ExactlyOne;
		}

		//This is hacky! Needs to be cleaned up as it makes many assumptions.
		static Type GetCollectionContractType(Type collectionType)
		{
			var itemType = collectionType.GetGenericArguments().First();
			var contractType = itemType.GetGenericArguments().First();
			return contractType;
		}

		static IEnumerable<ExportDefinition> GetExportDefinitions(Type implementationType, Type contractType, string key = null)
		{
			var lazyMember = new LazyMemberInfo(implementationType);
			var contracName = key ?? AttributedModelServices.GetContractName(contractType);
			var metadata = new Lazy<IDictionary<string, object>>(() =>
			                                                     	{
			                                                     		var md = new Dictionary<string, object> {
			                                                     		                                        	{ CompositionConstants.ExportTypeIdentityMetadataName, AttributedModelServices.GetTypeIdentity(contractType) }
			                                                     		                                        };
			                                                     		return md;
			                                                     	});

			return new[] {
			             	ReflectionModelServices.CreateExportDefinition(lazyMember, contracName, metadata, null)
			             };
		}
	}
}