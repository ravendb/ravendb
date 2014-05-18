package net.ravendb.abstractions.basic;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used - for serializing types using .NET enum conventions
 *
 * When enum is marked as flags and exists in EnumSet it must also contain method:
 * int getValue() which returns int value used in flag
 *
 */
@Target({ ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface SerializeUsingValue {

}
