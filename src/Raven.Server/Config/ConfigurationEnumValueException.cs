using System;
using System.Linq;

namespace Raven.Server.Config
{
    public class ConfigurationEnumValueException : Exception
    {
        private Type _enumType;

        private string _attemptedValue;

        private string _message;

        public ConfigurationEnumValueException(string attemptedValue, Type enumType)
            : base()
        {
            _attemptedValue = attemptedValue;
            _enumType = enumType;
        }

        public override string Message
        {
            get
            {
                if (string.IsNullOrEmpty(_message))
                    _message = FormatMessage();

                return _message;
            }
        }

        private string FormatMessage()
        {
            var isFlag = IsFlag();
            var usageText = isFlag
                ? "use combination of the following flags"  
                : "use one of the following values";
            var sep = isFlag ? " | " : ", ";
            return $"'{ _attemptedValue }' is an invalid value, { usageText }: {string.Join(sep, GetPossibleEnumValues())}.";
        }

        private string[] GetPossibleEnumValues()
        {
            return Enum.GetNames(_enumType);
        }

        private bool IsFlag()
        {
            return _enumType.GetCustomAttributes(typeof(FlagsAttribute), false).Any();

        }
    }
}
