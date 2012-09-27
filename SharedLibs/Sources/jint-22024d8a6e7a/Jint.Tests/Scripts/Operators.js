var i = 1;
var j = i++;
assert(1, j);
assert(2, i);

var i = 1;
var j = i--;
assert(1, j);
assert(0, i);

var i = 1;
var j = --i;
assert(0, j);
assert(0, i);

var i = 1;
var j = ++i;
assert(2, j);
assert(2, i);

var i = 1;
i += 1; assert(2, i);
i -= 1; assert(1, i);
i *= 2; assert(2, i);
i /= 2; assert(1, i);

var b = true;
b &= false; assert(0, b);
b |= true; assert(1, b);

assert(7, 1+2*3);
assert(27, 3*3*3);
assert(9, 81/9);
assert(-9, 81/-9);
assert(Infinity, 1/0);
assert(-Infinity, -1/0);
assert(0, 4%4);
assert(NaN, 0%0);
assert(1, 10%7%2);
assert(16, 8 << 1);
assert(4, 8 >> 1);
assert(16, 8 << 1);
assert(4, 8 >>> 1);
assert(true, 1<2);
assert(false, 1<1);
assert(true, 2>1);
assert(false, 1>1);
assert(0, 2^2);
assert(1, 3^2);
assert(1, 3&1);
assert(0, 3&4);
assert(3, 1|2);
var x=1;
assert(8, x <<= 3); assert(8, x);
assert(2, x >>= 2); assert(2, x);
assert(1, x >>= 1); assert(1, x);
assert(3, x |= 2); assert(3, x);
assert(0, x ^= 3); assert(0, x);
assert(0, x &= 0); assert(0, x);
assert(-9, ~8);

// Returns expr1 if it can be converted to false; otherwise, returns expr2. Thus, when used with Boolean values, && returns true if both operands are true; otherwise, returns false.
assert(true, true && true);       // t && t returns true
assert(false, true && false);      // t && f returns false
assert(false, false && true);      // f && t returns false
assert(false, false && (3 == 4));  // f && f returns false
assert("Dog", "Cat" && "Dog");     // t && t returns Dog
assert(false, false && "Cat");     // f && t returns false
assert(false, "Cat" && false);     // t && f returns false
assert(1, true && 1); 

// Returns expr1 if it can be converted to true; otherwise, returns expr2. Thus, when used with Boolean values, || returns true if either operand is true; if both are false, returns false.
assert(true, true || true);       // t || t returns true
assert(true, false || true);      // f || t returns true
assert(true, true || false);      // t || f returns true
assert(false, false || (3 == 4));  // f || f returns false
assert("Cat", "Cat" || "Dog");     // t || t returns Cat
assert("Cat", false || "Cat");     // f || t returns Cat
assert("Cat", "Cat" || false);     // t || f returns Cat
assert(1, false || 1);     
assert(1, null || 1);     
assert(1, undefined || 1);     

assert(false, !true);              // !t returns false
assert(true, !false);             // !f returns true
assert(false, !"Cat");             // !t returns false

assert(undefined | 1, 1);
assert(undefined & 1, 0);
assert(undefined ^ 1, 1);
assert(undefined << 1, 0);
assert(undefined >> 1, 0);
assert(undefined >>> 1, 0);

assert(1 | undefined, 1);
assert(1 & undefined, 0);
assert(1 ^ undefined, 1);
assert(1 << undefined, 1);
assert(1 >> undefined, 1);
assert(1 >>> undefined, 1);

assert(undefined | undefined, 1);
assert(undefined & undefined, 0);
assert(undefined ^ undefined, 1);
assert(undefined << undefined, 0);
assert(undefined >> undefined, 0);
assert(undefined >>> undefined, 0);

assert(undefined, void 0);
assert(undefined, void(42));
var myNum;
assert(undefined, void(myNum=3));
assert(3, myNum);
