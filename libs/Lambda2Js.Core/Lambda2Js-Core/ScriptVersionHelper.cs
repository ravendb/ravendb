using System;

namespace Lambda2Js
{
    public static class ScriptVersionHelper
    {
        /// <summary>
        /// Indicates whether the specified version of JavaScript supports the given syntax or syntax.
        /// </summary>
        /// <param name="scriptVersion"></param>
        /// <param name="syntax"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public static bool Supports(this ScriptVersion scriptVersion, JavascriptSyntax syntax)
        {
            switch (syntax)
            {
                case JavascriptSyntax.ArrowFunction:
                    switch (scriptVersion)
                    {
                        case ScriptVersion.Es30:
                        case ScriptVersion.Es50:
                            return false;
                        case ScriptVersion.Es60:
                        case ScriptVersion.Es70:
                            return true;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(scriptVersion));
                    }
                case JavascriptSyntax.ArraySpread:
                    switch (scriptVersion)
                    {
                        case ScriptVersion.Es30:
                        case ScriptVersion.Es50:
                            return false;
                        case ScriptVersion.Es60:
                        case ScriptVersion.Es70:
                            return true;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(scriptVersion));
                    };
                default:
                    throw new ArgumentOutOfRangeException(nameof(syntax));
            }
        }
    }
}