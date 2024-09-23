namespace Raven.Server.SchemaValidation;

// internal class ValueSchemaRuleValidatorFactory(Type classType)
// {
//     public ISchemaRuleValidator<T> Create<T>(string path, object[] args)
//     {
//         var ctor = classType.GetConstructors().FirstOrDefault(x => x.GetParameters().Length == args.Length + 1);
//         if (ctor == null)
//             //TODO To log the error up
//             throw new Exception();
//
//         var ctorParams = new List<object>{path};
//         var ctorParamInfo = ctor.GetParameters();
//         for (int i = 0; i < ctorParamInfo.Length; i++)
//         {
//             var param = Convert.ChangeType(args[i], ctorParamInfo[i].ParameterType, CultureInfo.InvariantCulture);
//             ctorParams.Add(param);
//         }
//
//         return (SchemaRuleValidator<T>)ctor.Invoke(ctorParams.ToArray());
//     }
// }
