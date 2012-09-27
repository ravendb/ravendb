using System;
using System.Collections.Generic;
using System.Text;

namespace Jint {
    public class Number {
        public static object Add(object a, object b) {
            TypeCode typeCodeA = Type.GetTypeCode(a.GetType());
            TypeCode typeCodeB = Type.GetTypeCode(b.GetType());

            switch (typeCodeA) {
                case TypeCode.Byte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (Byte)a + (SByte)b;
                        case TypeCode.Int16: return (Byte)a + (Int16)b;
                        case TypeCode.UInt16: return (Byte)a + (UInt16)b;
                        case TypeCode.Int32: return (Byte)a + (Int32)b;
                        case TypeCode.UInt32: return (Byte)a + (UInt32)b;
                        case TypeCode.Int64: return (Byte)a + (Int64)b;
                        case TypeCode.UInt64: return (Byte)a + (UInt64)b;
                        case TypeCode.Single: return (Byte)a + (Single)b;
                        case TypeCode.Double: return (Byte)a + (Double)b;
                        case TypeCode.Decimal: return (Byte)a + (Decimal)b;
                    }
                    break;
                case TypeCode.SByte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (SByte)a + (SByte)b;
                        case TypeCode.Int16: return (SByte)a + (Int16)b;
                        case TypeCode.UInt16: return (SByte)a + (UInt16)b;
                        case TypeCode.Int32: return (SByte)a + (Int32)b;
                        case TypeCode.UInt32: return (SByte)a + (UInt32)b;
                        case TypeCode.Int64: return (SByte)a + (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '+' can't be applied to operands of types 'sbyte' and 'ulong'"); ;
                        case TypeCode.Single: return (SByte)a + (Single)b;
                        case TypeCode.Double: return (SByte)a + (Double)b;
                        case TypeCode.Decimal: return (SByte)a + (Decimal)b;
                    }
                    break;

                case TypeCode.Int16:
                    switch (typeCodeB) {
                        case TypeCode.Int16: return (Int16)a + (Int16)b;
                        case TypeCode.UInt16: return (Int16)a + (UInt16)b;
                        case TypeCode.Int32: return (Int16)a + (Int32)b;
                        case TypeCode.UInt32: return (Int16)a + (UInt32)b;
                        case TypeCode.Int64: return (Int16)a + (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '+' can't be applied to operands of types 'short' and 'ulong'"); ;
                        case TypeCode.Single: return (Int16)a + (Single)b;
                        case TypeCode.Double: return (Int16)a + (Double)b;
                        case TypeCode.Decimal: return (Int16)a + (Decimal)b;
                    }
                    break;

                case TypeCode.UInt16:
                    switch (typeCodeB) {
                        case TypeCode.UInt16: return (UInt16)a + (UInt16)b;
                        case TypeCode.Int32: return (UInt16)a + (Int32)b;
                        case TypeCode.UInt32: return (UInt16)a + (UInt32)b;
                        case TypeCode.Int64: return (UInt16)a + (Int64)b;
                        case TypeCode.UInt64: return (UInt16)a + (UInt64)b;
                        case TypeCode.Single: return (UInt16)a + (Single)b;
                        case TypeCode.Double: return (UInt16)a + (Double)b;
                        case TypeCode.Decimal: return (UInt16)a + (Decimal)b;
                    }
                    break;

                case TypeCode.Int32:
                    switch (typeCodeB) {
                        case TypeCode.Int32: return (Int32)a + (Int32)b;
                        case TypeCode.UInt32: return (Int32)a + (UInt32)b;
                        case TypeCode.Int64: return (Int32)a + (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '+' can't be applied to operands of types 'int' and 'ulong'"); ;
                        case TypeCode.Single: return (Int32)a + (Single)b;
                        case TypeCode.Double: return (Int32)a + (Double)b;
                        case TypeCode.Decimal: return (Int32)a + (Decimal)b;
                    }
                    break;

                case TypeCode.UInt32:
                    switch (typeCodeB) {
                        case TypeCode.UInt32: return (UInt32)a + (UInt32)b;
                        case TypeCode.Int64: return (UInt32)a + (Int64)b;
                        case TypeCode.UInt64: return (UInt32)a + (UInt64)b;
                        case TypeCode.Single: return (UInt32)a + (Single)b;
                        case TypeCode.Double: return (UInt32)a + (Double)b;
                        case TypeCode.Decimal: return (UInt32)a + (Decimal)b;
                    }
                    break;

                case TypeCode.Int64:
                    switch (typeCodeB) {
                        case TypeCode.Int64: return (Int64)a + (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '+' can't be applied to operands of types 'long' and 'ulong'"); ;
                        case TypeCode.Single: return (Int64)a + (Single)b;
                        case TypeCode.Double: return (Int64)a + (Double)b;
                        case TypeCode.Decimal: return (Int64)a + (Decimal)b;
                    }
                    break;

                case TypeCode.UInt64:
                    switch (typeCodeB) {
                        case TypeCode.UInt64: return (UInt64)a + (UInt64)b;
                        case TypeCode.Single: return (UInt64)a + (Single)b;
                        case TypeCode.Double: return (UInt64)a + (Double)b;
                        case TypeCode.Decimal: return (UInt64)a + (Decimal)b;
                    }
                    break;

                case TypeCode.Single:
                    switch (typeCodeB) {
                        case TypeCode.Single: return (Single)a + (Single)b;
                        case TypeCode.Double: return (Single)a + (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '+' can't be applied to operands of types 'float' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Double:
                    switch (typeCodeB) {
                        case TypeCode.Double: return (Double)a + (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '+' can't be applied to operands of types 'double' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Decimal:
                    switch (typeCodeB) {
                        case TypeCode.Decimal: return (Decimal)a + (Decimal)b;
                    }
                    break;
            }

            return null;
        }
        public static object Soustract(object a, object b) {
            TypeCode typeCodeA = Type.GetTypeCode(a.GetType());
            TypeCode typeCodeB = Type.GetTypeCode(b.GetType());

            switch (typeCodeA) {
                case TypeCode.Byte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (Byte)a - (SByte)b;
                        case TypeCode.Int16: return (Byte)a - (Int16)b;
                        case TypeCode.UInt16: return (Byte)a - (UInt16)b;
                        case TypeCode.Int32: return (Byte)a - (Int32)b;
                        case TypeCode.UInt32: return (Byte)a - (UInt32)b;
                        case TypeCode.Int64: return (Byte)a - (Int64)b;
                        case TypeCode.UInt64: return (Byte)a - (UInt64)b;
                        case TypeCode.Single: return (Byte)a - (Single)b;
                        case TypeCode.Double: return (Byte)a - (Double)b;
                        case TypeCode.Decimal: return (Byte)a - (Decimal)b;
                    }
                    break;
                case TypeCode.SByte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (SByte)a - (SByte)b;
                        case TypeCode.Int16: return (SByte)a - (Int16)b;
                        case TypeCode.UInt16: return (SByte)a - (UInt16)b;
                        case TypeCode.Int32: return (SByte)a - (Int32)b;
                        case TypeCode.UInt32: return (SByte)a - (UInt32)b;
                        case TypeCode.Int64: return (SByte)a - (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '-' can't be applied to operands of types 'sbyte' and 'ulong'"); ;
                        case TypeCode.Single: return (SByte)a - (Single)b;
                        case TypeCode.Double: return (SByte)a - (Double)b;
                        case TypeCode.Decimal: return (SByte)a - (Decimal)b;
                    }
                    break;

                case TypeCode.Int16:
                    switch (typeCodeB) {
                        case TypeCode.Int16: return (Int16)a - (Int16)b;
                        case TypeCode.UInt16: return (Int16)a - (UInt16)b;
                        case TypeCode.Int32: return (Int16)a - (Int32)b;
                        case TypeCode.UInt32: return (Int16)a - (UInt32)b;
                        case TypeCode.Int64: return (Int16)a - (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '-' can't be applied to operands of types 'short' and 'ulong'"); ;
                        case TypeCode.Single: return (Int16)a - (Single)b;
                        case TypeCode.Double: return (Int16)a - (Double)b;
                        case TypeCode.Decimal: return (Int16)a - (Decimal)b;
                    }
                    break;

                case TypeCode.UInt16:
                    switch (typeCodeB) {
                        case TypeCode.UInt16: return (UInt16)a - (UInt16)b;
                        case TypeCode.Int32: return (UInt16)a - (Int32)b;
                        case TypeCode.UInt32: return (UInt16)a - (UInt32)b;
                        case TypeCode.Int64: return (UInt16)a - (Int64)b;
                        case TypeCode.UInt64: return (UInt16)a - (UInt64)b;
                        case TypeCode.Single: return (UInt16)a - (Single)b;
                        case TypeCode.Double: return (UInt16)a - (Double)b;
                        case TypeCode.Decimal: return (UInt16)a - (Decimal)b;
                    }
                    break;

                case TypeCode.Int32:
                    switch (typeCodeB) {
                        case TypeCode.Int32: return (Int32)a - (Int32)b;
                        case TypeCode.UInt32: return (Int32)a - (UInt32)b;
                        case TypeCode.Int64: return (Int32)a - (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '-' can't be applied to operands of types 'int' and 'ulong'"); ;
                        case TypeCode.Single: return (Int32)a - (Single)b;
                        case TypeCode.Double: return (Int32)a - (Double)b;
                        case TypeCode.Decimal: return (Int32)a - (Decimal)b;
                    }
                    break;

                case TypeCode.UInt32:
                    switch (typeCodeB) {
                        case TypeCode.UInt32: return (UInt32)a - (UInt32)b;
                        case TypeCode.Int64: return (UInt32)a - (Int64)b;
                        case TypeCode.UInt64: return (UInt32)a - (UInt64)b;
                        case TypeCode.Single: return (UInt32)a - (Single)b;
                        case TypeCode.Double: return (UInt32)a - (Double)b;
                        case TypeCode.Decimal: return (UInt32)a - (Decimal)b;
                    }
                    break;

                case TypeCode.Int64:
                    switch (typeCodeB) {
                        case TypeCode.Int64: return (Int64)a - (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '-' can't be applied to operands of types 'long' and 'ulong'"); ;
                        case TypeCode.Single: return (Int64)a - (Single)b;
                        case TypeCode.Double: return (Int64)a - (Double)b;
                        case TypeCode.Decimal: return (Int64)a - (Decimal)b;
                    }
                    break;

                case TypeCode.UInt64:
                    switch (typeCodeB) {
                        case TypeCode.UInt64: return (UInt64)a - (UInt64)b;
                        case TypeCode.Single: return (UInt64)a - (Single)b;
                        case TypeCode.Double: return (UInt64)a - (Double)b;
                        case TypeCode.Decimal: return (UInt64)a - (Decimal)b;
                    }
                    break;

                case TypeCode.Single:
                    switch (typeCodeB) {
                        case TypeCode.Single: return (Single)a - (Single)b;
                        case TypeCode.Double: return (Single)a - (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '-' can't be applied to operands of types 'float' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Double:
                    switch (typeCodeB) {
                        case TypeCode.Double: return (Double)a - (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '-' can't be applied to operands of types 'double' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Decimal:
                    switch (typeCodeB) {
                        case TypeCode.Decimal: return (Byte)a - (Decimal)b;
                    }
                    break;
            }

            return null;
        }
        public static object Multiply(object a, object b) {
            TypeCode typeCodeA = Type.GetTypeCode(a.GetType());
            TypeCode typeCodeB = Type.GetTypeCode(b.GetType());

            switch (typeCodeA) {
                case TypeCode.Byte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (Byte)a * (SByte)b;
                        case TypeCode.Int16: return (Byte)a * (Int16)b;
                        case TypeCode.UInt16: return (Byte)a * (UInt16)b;
                        case TypeCode.Int32: return (Byte)a * (Int32)b;
                        case TypeCode.UInt32: return (Byte)a * (UInt32)b;
                        case TypeCode.Int64: return (Byte)a * (Int64)b;
                        case TypeCode.UInt64: return (Byte)a * (UInt64)b;
                        case TypeCode.Single: return (Byte)a * (Single)b;
                        case TypeCode.Double: return (Byte)a * (Double)b;
                        case TypeCode.Decimal: return (Byte)a * (Decimal)b;
                    }
                    break;
                case TypeCode.SByte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (SByte)a * (SByte)b;
                        case TypeCode.Int16: return (SByte)a * (Int16)b;
                        case TypeCode.UInt16: return (SByte)a * (UInt16)b;
                        case TypeCode.Int32: return (SByte)a * (Int32)b;
                        case TypeCode.UInt32: return (SByte)a * (UInt32)b;
                        case TypeCode.Int64: return (SByte)a * (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '*' can't be applied to operands of types 'sbyte' and 'ulong'"); ;
                        case TypeCode.Single: return (SByte)a * (Single)b;
                        case TypeCode.Double: return (SByte)a * (Double)b;
                        case TypeCode.Decimal: return (SByte)a * (Decimal)b;
                    }
                    break;

                case TypeCode.Int16:
                    switch (typeCodeB) {
                        case TypeCode.Int16: return (Int16)a * (Int16)b;
                        case TypeCode.UInt16: return (Int16)a * (UInt16)b;
                        case TypeCode.Int32: return (Int16)a * (Int32)b;
                        case TypeCode.UInt32: return (Int16)a * (UInt32)b;
                        case TypeCode.Int64: return (Int16)a * (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '*' can't be applied to operands of types 'short' and 'ulong'"); ;
                        case TypeCode.Single: return (Int16)a * (Single)b;
                        case TypeCode.Double: return (Int16)a * (Double)b;
                        case TypeCode.Decimal: return (Int16)a * (Decimal)b;
                    }
                    break;

                case TypeCode.UInt16:
                    switch (typeCodeB) {
                        case TypeCode.UInt16: return (UInt16)a * (UInt16)b;
                        case TypeCode.Int32: return (UInt16)a * (Int32)b;
                        case TypeCode.UInt32: return (UInt16)a * (UInt32)b;
                        case TypeCode.Int64: return (UInt16)a * (Int64)b;
                        case TypeCode.UInt64: return (UInt16)a * (UInt64)b;
                        case TypeCode.Single: return (UInt16)a * (Single)b;
                        case TypeCode.Double: return (UInt16)a * (Double)b;
                        case TypeCode.Decimal: return (UInt16)a * (Decimal)b;
                    }
                    break;

                case TypeCode.Int32:
                    switch (typeCodeB) {
                        case TypeCode.Int32: return (Int32)a * (Int32)b;
                        case TypeCode.UInt32: return (Int32)a * (UInt32)b;
                        case TypeCode.Int64: return (Int32)a * (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '*' can't be applied to operands of types 'int' and 'ulong'"); ;
                        case TypeCode.Single: return (Int32)a * (Single)b;
                        case TypeCode.Double: return (Int32)a * (Double)b;
                        case TypeCode.Decimal: return (Int32)a * (Decimal)b;
                    }
                    break;

                case TypeCode.UInt32:
                    switch (typeCodeB) {
                        case TypeCode.UInt32: return (UInt32)a * (UInt32)b;
                        case TypeCode.Int64: return (UInt32)a * (Int64)b;
                        case TypeCode.UInt64: return (UInt32)a * (UInt64)b;
                        case TypeCode.Single: return (UInt32)a * (Single)b;
                        case TypeCode.Double: return (UInt32)a * (Double)b;
                        case TypeCode.Decimal: return (UInt32)a * (Decimal)b;
                    }
                    break;

                case TypeCode.Int64:
                    switch (typeCodeB) {
                        case TypeCode.Int64: return (Int64)a * (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '*' can't be applied to operands of types 'long' and 'ulong'"); ;
                        case TypeCode.Single: return (Int64)a * (Single)b;
                        case TypeCode.Double: return (Int64)a * (Double)b;
                        case TypeCode.Decimal: return (Int64)a * (Decimal)b;
                    }
                    break;

                case TypeCode.UInt64:
                    switch (typeCodeB) {
                        case TypeCode.UInt64: return (UInt64)a * (UInt64)b;
                        case TypeCode.Single: return (UInt64)a * (Single)b;
                        case TypeCode.Double: return (UInt64)a * (Double)b;
                        case TypeCode.Decimal: return (UInt64)a * (Decimal)b;
                    }
                    break;

                case TypeCode.Single:
                    switch (typeCodeB) {
                        case TypeCode.Single: return (Single)a * (Single)b;
                        case TypeCode.Double: return (Single)a * (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '*' can't be applied to operands of types 'float' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Double:
                    switch (typeCodeB) {
                        case TypeCode.Double: return (Double)a * (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '*' can't be applied to operands of types 'double' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Decimal:
                    switch (typeCodeB) {
                        case TypeCode.Decimal: return (Byte)a * (Decimal)b;
                    }
                    break;
            }

            return null;
        }
        public static object Divide(object a, object b) {
            TypeCode typeCodeA = Type.GetTypeCode(a.GetType());
            TypeCode typeCodeB = Type.GetTypeCode(b.GetType());

            switch (typeCodeA) {
                case TypeCode.Byte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (Byte)a / (SByte)b;
                        case TypeCode.Int16: return (Byte)a / (Int16)b;
                        case TypeCode.UInt16: return (Byte)a / (UInt16)b;
                        case TypeCode.Int32: return (Byte)a / (Int32)b;
                        case TypeCode.UInt32: return (Byte)a / (UInt32)b;
                        case TypeCode.Int64: return (Byte)a / (Int64)b;
                        case TypeCode.UInt64: return (Byte)a / (UInt64)b;
                        case TypeCode.Single: return (Byte)a / (Single)b;
                        case TypeCode.Double: return (Byte)a / (Double)b;
                        case TypeCode.Decimal: return (Byte)a / (Decimal)b;
                    }
                    break;
                case TypeCode.SByte:
                    switch (typeCodeB) {
                        case TypeCode.SByte: return (SByte)a / (SByte)b;
                        case TypeCode.Int16: return (SByte)a / (Int16)b;
                        case TypeCode.UInt16: return (SByte)a / (UInt16)b;
                        case TypeCode.Int32: return (SByte)a / (Int32)b;
                        case TypeCode.UInt32: return (SByte)a / (UInt32)b;
                        case TypeCode.Int64: return (SByte)a / (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '/' can't be applied to operands of types 'sbyte' and 'ulong'"); ;
                        case TypeCode.Single: return (SByte)a / (Single)b;
                        case TypeCode.Double: return (SByte)a / (Double)b;
                        case TypeCode.Decimal: return (SByte)a / (Decimal)b;
                    }
                    break;

                case TypeCode.Int16:
                    switch (typeCodeB) {
                        case TypeCode.Int16: return (Int16)a / (Int16)b;
                        case TypeCode.UInt16: return (Int16)a / (UInt16)b;
                        case TypeCode.Int32: return (Int16)a / (Int32)b;
                        case TypeCode.UInt32: return (Int16)a / (UInt32)b;
                        case TypeCode.Int64: return (Int16)a / (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '/' can't be applied to operands of types 'short' and 'ulong'"); ;
                        case TypeCode.Single: return (Int16)a / (Single)b;
                        case TypeCode.Double: return (Int16)a / (Double)b;
                        case TypeCode.Decimal: return (Int16)a / (Decimal)b;
                    }
                    break;

                case TypeCode.UInt16:
                    switch (typeCodeB) {
                        case TypeCode.UInt16: return (UInt16)a / (UInt16)b;
                        case TypeCode.Int32: return (UInt16)a / (Int32)b;
                        case TypeCode.UInt32: return (UInt16)a / (UInt32)b;
                        case TypeCode.Int64: return (UInt16)a / (Int64)b;
                        case TypeCode.UInt64: return (UInt16)a / (UInt64)b;
                        case TypeCode.Single: return (UInt16)a / (Single)b;
                        case TypeCode.Double: return (UInt16)a / (Double)b;
                        case TypeCode.Decimal: return (UInt16)a / (Decimal)b;
                    }
                    break;

                case TypeCode.Int32:
                    switch (typeCodeB) {
                        case TypeCode.Int32: return (Int32)a / (Int32)b;
                        case TypeCode.UInt32: return (Int32)a / (UInt32)b;
                        case TypeCode.Int64: return (Int32)a / (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '/' can't be applied to operands of types 'int' and 'ulong'"); ;
                        case TypeCode.Single: return (Int32)a / (Single)b;
                        case TypeCode.Double: return (Int32)a / (Double)b;
                        case TypeCode.Decimal: return (Int32)a / (Decimal)b;
                    }
                    break;

                case TypeCode.UInt32:
                    switch (typeCodeB) {
                        case TypeCode.UInt32: return (UInt32)a / (UInt32)b;
                        case TypeCode.Int64: return (UInt32)a / (Int64)b;
                        case TypeCode.UInt64: return (UInt32)a / (UInt64)b;
                        case TypeCode.Single: return (UInt32)a / (Single)b;
                        case TypeCode.Double: return (UInt32)a / (Double)b;
                        case TypeCode.Decimal: return (UInt32)a / (Decimal)b;
                    }
                    break;

                case TypeCode.Int64:
                    switch (typeCodeB) {
                        case TypeCode.Int64: return (Int64)a / (Int64)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '/' can't be applied to operands of types 'long' and 'ulong'"); ;
                        case TypeCode.Single: return (Int64)a / (Single)b;
                        case TypeCode.Double: return (Int64)a / (Double)b;
                        case TypeCode.Decimal: return (Int64)a / (Decimal)b;
                    }
                    break;

                case TypeCode.UInt64:
                    switch (typeCodeB) {
                        case TypeCode.UInt64: return (UInt64)a / (UInt64)b;
                        case TypeCode.Single: return (UInt64)a / (Single)b;
                        case TypeCode.Double: return (UInt64)a / (Double)b;
                        case TypeCode.Decimal: return (UInt64)a / (Decimal)b;
                    }
                    break;

                case TypeCode.Single:
                    switch (typeCodeB) {
                        case TypeCode.Single: return (Single)a / (Single)b;
                        case TypeCode.Double: return (Single)a / (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '/' can't be applied to operands of types 'float' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Double:
                    switch (typeCodeB) {
                        case TypeCode.Double: return (Double)a / (Double)b;
                        case TypeCode.UInt64: throw new InvalidOperationException("Operator '/' can't be applied to operands of types 'double' and 'decimal'"); ;
                    }
                    break;

                case TypeCode.Decimal:
                    switch (typeCodeB) {
                        case TypeCode.Decimal: return (Byte)a / (Decimal)b;
                    }
                    break;
            }

            return null;
        }
        public static object Max(object a, object b) {
            if (a == null && b == null) {
                return null;
            }

            if (a == null) {
                return b;
            }

            if (b == null) {
                return a;
            }

            TypeCode typeCodeA = Type.GetTypeCode(a.GetType());
            TypeCode typeCodeB = Type.GetTypeCode(b.GetType());

            switch (typeCodeA) {
                case TypeCode.Byte:
                    return Math.Max((Byte)a, Convert.ToByte(b));
                case TypeCode.SByte:
                    return Math.Max((SByte)a, Convert.ToSByte(b));
                case TypeCode.Int16:
                    return Math.Max((Int16)a, Convert.ToInt16(b));
                case TypeCode.UInt16:
                    return Math.Max((UInt16)a, Convert.ToUInt16(b));
                case TypeCode.Int32:
                    return Math.Max((Int32)a, Convert.ToInt32(b));
                case TypeCode.UInt32:
                    return Math.Max((UInt32)a, Convert.ToUInt32(b));
                case TypeCode.Int64:
                    return Math.Max((Int64)a, Convert.ToInt64(b));
                case TypeCode.UInt64:
                    return Math.Max((UInt64)a, Convert.ToUInt64(b));
                case TypeCode.Single:
                    return Math.Max((Single)a, Convert.ToSingle(b));
                case TypeCode.Double:
                    return Math.Max((Double)a, Convert.ToDouble(b));
                case TypeCode.Decimal:
                    return Math.Max((Decimal)a, Convert.ToDecimal(b));
            }

            return null;
        }
        public static object Min(object a, object b) {
            if (a == null && b == null) {
                return null;
            }

            if (a == null) {
                return b;
            }

            if (b == null) {
                return a;
            }

            TypeCode typeCodeA = Type.GetTypeCode(a.GetType());
            TypeCode typeCodeB = Type.GetTypeCode(b.GetType());

            switch (typeCodeA) {
                case TypeCode.Byte:
                    return Math.Min((Byte)a, Convert.ToByte(b));
                case TypeCode.SByte:
                    return Math.Min((SByte)a, Convert.ToSByte(b));
                case TypeCode.Int16:
                    return Math.Min((Int16)a, Convert.ToInt16(b));
                case TypeCode.UInt16:
                    return Math.Min((UInt16)a, Convert.ToUInt16(b));
                case TypeCode.Int32:
                    return Math.Min((Int32)a, Convert.ToInt32(b));
                case TypeCode.UInt32:
                    return Math.Min((UInt32)a, Convert.ToUInt32(b));
                case TypeCode.Int64:
                    return Math.Min((Int64)a, Convert.ToInt64(b));
                case TypeCode.UInt64:
                    return Math.Min((UInt64)a, Convert.ToUInt64(b));
                case TypeCode.Single:
                    return Math.Min((Single)a, Convert.ToSingle(b));
                case TypeCode.Double:
                    return Math.Min((Double)a, Convert.ToDouble(b));
                case TypeCode.Decimal:
                    return Math.Min((Decimal)a, Convert.ToDecimal(b));
            }

            return null;
        }

    }
}
