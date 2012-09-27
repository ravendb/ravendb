assert(false, new Boolean());
assert(false, new Boolean(0));
assert(false, new Boolean(null));
assert(false, new Boolean(""));
assert(false, new Boolean(false));

assert(true, new Boolean(true));
assert(true, new Boolean("true"));
assert(true, new Boolean("false"));
assert(true, new Boolean("Su Lin"));

// properties on literals should not be remembered
var bool = true;
bool.is_false = false;
assert(undefined, bool.is_false);